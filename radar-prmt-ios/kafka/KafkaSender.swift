//
//  KafkaSender.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
import os.log

class KafkaSender: NSObject, URLSessionTaskDelegate, URLSessionDataDelegate {
    let context: KafkaSendContext
    let auth: Authorizer
    var queue: DispatchQueue!
    let baseUrl: URL
    var highPrioritySession: URLSession!
    var lowPrioritySession: URLSession!
    var highPrioritySessionCompletionHandler: (() -> Void)? = nil
    var lowPrioritySessionCompletionHandler: (() -> Void)? = nil

    init(baseUrl: URL, context: KafkaSendContext, auth: Authorizer) {
        var kafkaUrl = baseUrl
        kafkaUrl.appendPathComponent("kafka", isDirectory: true)
        kafkaUrl.appendPathComponent("topics", isDirectory: true)
        self.baseUrl = kafkaUrl
        self.context = context
        self.auth = auth
        super.init()
    }

    public func start() {
        guard queue == nil else { return }

        queue = DispatchQueue(label: "KafkaSender", qos: .background)
        let operationQueue = OperationQueue()
        operationQueue.underlyingQueue = queue

        var sessionConfig = URLSessionConfiguration.background(withIdentifier: "kafkaSenderHighPriority")
        sessionConfig.waitsForConnectivity = true
        sessionConfig.allowsCellularAccess = true
        highPrioritySession = URLSession(configuration: sessionConfig, delegate: self, delegateQueue: operationQueue)

        sessionConfig = URLSessionConfiguration.background(withIdentifier: "kafkaSenderLowPriority")
        sessionConfig.waitsForConnectivity = true
        sessionConfig.allowsCellularAccess = false
        lowPrioritySession = URLSession(configuration: sessionConfig, delegate: self, delegateQueue: operationQueue)
    }

    func send(handle: UploadHandle, priority: Int16) {
        assert(queue != nil, "Cannot send data without starting KafkaSender")
        let session: URLSession! = priority >= self.context.minimumPriorityForCellular ? self.highPrioritySession : self.lowPrioritySession

        var request = URLRequest(url: baseUrl.appendingPathComponent(handle.topic, isDirectory: false))
        self.auth.addAuthorization(to: &request)
        let uploadTask = session.uploadTask(with: request, from: handle)
        uploadTask.resume()
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        guard let topic = task.originalRequest?.url?.extractTopic() else {
            os_log("Cannot extract log from request %@", task.originalRequest?.url?.absoluteString ?? "")
            return
        }

        if let error = error {
            let nsError = error as NSError
            switch nsError.code {
            case NSURLErrorInternationalRoamingOff,
                 NSURLErrorCallIsActive,
                 NSURLErrorDataNotAllowed,
                 NSURLErrorNotConnectedToInternet,
                 NSURLErrorNetworkConnectionLost:
                let network: NetworkReachability.Mode = session == highPrioritySession ? [.cellular, .wifiOrEthernet] : .wifiOrEthernet
                context.couldNotConnect(with: topic, over: network)
            default:
                context.serverFailure(for: topic)
            }
            return
        }

        guard let urlResponse = task.response, let response = urlResponse as? HTTPURLResponse else {
            os_log("No response present")
            context.serverFailure(for: topic)
            return
        }

        switch response.statusCode {
        case 200 ..< 300:
            context.didSucceed(for: topic)
        case 401, 403:
            os_log("Authentication with RADAR-base failed.")
            auth.invalidate()
            context.mayRetry(topic: topic)
        case 400 ..< 500:
            os_log("Failed code %d", type: .error, response.statusCode)
            context.didFail(for: topic, code: Int16(response.statusCode), message: "Upload failed", recoverable: true)
        default:
            context.serverFailure(for: topic)
        }

    }

    func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
        let completionHandler: (() -> Void)?
        switch session {
        case highPrioritySession:
            completionHandler = highPrioritySessionCompletionHandler
        case lowPrioritySession:
            completionHandler = lowPrioritySessionCompletionHandler
        default:
            completionHandler = nil
        }
        if let completionHandler = completionHandler {
            DispatchQueue.main.async {
                completionHandler()
            }
        }
    }
}

extension URL {
    func extractTopic() -> String {
        return lastPathComponent
    }
}
