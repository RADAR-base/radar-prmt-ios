//
//  KafkaSender.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
import RxSwift
import os.log

class KafkaSender: NSObject, URLSessionTaskDelegate, URLSessionDataDelegate {
    let context: KafkaSendContext
    let disposeBag = DisposeBag()
    let authController: AuthController
    var queue: DispatchQueue!
    var rxQueue: SchedulerType!
    let baseUrl: URL
    var highPrioritySession: URLSession!
    var lowPrioritySession: URLSession!
    var highPrioritySessionCompletionHandler: (() -> Void)? = nil
    var lowPrioritySessionCompletionHandler: (() -> Void)? = nil
    var receivedData = [URLSessionTask: Data]()

    init(authController: AuthController, user: User, context: KafkaSendContext) {
        var kafkaUrl = user.baseUrl
        kafkaUrl.appendPathComponent("kafka", isDirectory: true)
        kafkaUrl.appendPathComponent("topics", isDirectory: true)
        self.baseUrl = kafkaUrl
        self.context = context
        self.authController = authController
        super.init()
    }

    public func start() {
        guard queue == nil else { return }

        queue = DispatchQueue(label: "KafkaSender", qos: .background)
        let operationQueue = OperationQueue()
        operationQueue.underlyingQueue = queue
        rxQueue = SerialDispatchQueueScheduler(queue: queue, internalSerialQueueName: "RxKakfaSender")

        var sessionConfig = URLSessionConfiguration.background(withIdentifier: "kafkaSenderHighPriority")
        sessionConfig.waitsForConnectivity = true
        sessionConfig.allowsCellularAccess = true
        highPrioritySession = URLSession(configuration: sessionConfig, delegate: self, delegateQueue: operationQueue)

        sessionConfig = URLSessionConfiguration.background(withIdentifier: "kafkaSenderLowPriority")
        sessionConfig.waitsForConnectivity = true
        sessionConfig.allowsCellularAccess = false
        lowPrioritySession = URLSession(configuration: sessionConfig, delegate: self, delegateQueue: operationQueue)
    }

    func send(handle: UploadHandle) {
        assert(queue != nil, "Cannot send data without starting KafkaSender")
        self.authController.validAuthentication()
            .take(1)
            .subscribeOn(rxQueue)
            .map { [weak self] auth -> URLSessionUploadTask in
                guard let self = self else {
                    throw MPAuthError.unreferenced
                }
                let session: URLSession! = handle.priority >= self.context.minimumPriorityForCellular ? self.highPrioritySession : self.lowPrioritySession

                var request = URLRequest(url: self.baseUrl.appendingPathComponent(handle.topic, isDirectory: false))
                try auth.addAuthorization(to: &request)
                return session.uploadTask(with: request, from: handle)
            }
            .subscribe(onNext: { [weak self] uploadTask in
                guard let self = self else { return }
                self.receivedData[uploadTask] = Data()
                uploadTask.resume()
            })
            .disposed(by: disposeBag)
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
                os_log("Network unavailable for request to Kafka: %@", type: .error, error.localizedDescription)
                let network: NetworkReachability.Mode = session == highPrioritySession ? [.cellular, .wifiOrEthernet] : .wifiOrEthernet
                context.couldNotConnect(with: topic, over: network)
            default:
                context.serverFailure(for: topic, message: error.localizedDescription)
            }
            return
        }

        guard let urlResponse = task.response, let response = urlResponse as? HTTPURLResponse else {
            context.serverFailure(for: topic, message: "No response present")
            return
        }

        if let responseBody = receivedData[task] {
            os_log("Response: %@", String(data: responseBody, encoding: .utf8) ?? "??")
            receivedData.removeValue(forKey: task)
        }
        switch response.statusCode {
        case 200 ..< 300:
            context.didSucceed(for: topic)
        case 401, 403:
            os_log("Authentication with RADAR-base failed.")
            self.authController.invalidate(accessToken: task.originalRequest?.value(forHTTPHeaderField: "Authorization") ?? "")
            context.mayRetry(topic: topic)
        case 400 ..< 500:
            context.didFail(for: topic, code: Int16(response.statusCode), message: "Upload failed with code \(response.statusCode)", recoverable: true)
        default:
            context.serverFailure(for: topic, message: "Failed to make request to Kafka with HTTP status code \(response.statusCode)")
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

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
        receivedData[dataTask]?.append(data)
    }
}

extension URL {
    func extractTopic() -> String {
        return lastPathComponent
    }
}
