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
    let schemaRegistry: SchemaRegistryClient
    let baseUrl: URL
    let bodyEncoder: KafkaRequestEncoder
    var highPrioritySession: URLSession!
    var lowPrioritySession: URLSession!
    var highPrioritySessionCompletionHandler: (() -> Void)? = nil
    var lowPrioritySessionCompletionHandler: (() -> Void)? = nil

    init?(baseUrl: URL, context: KafkaSendContext, auth: Authorizer) {
        var kafkaUrl = baseUrl
        kafkaUrl.appendPathComponent("kafka", isDirectory: true)
        kafkaUrl.appendPathComponent("topics", isDirectory: true)
        self.baseUrl = kafkaUrl
        self.context = context
        self.auth = auth
        self.schemaRegistry = SchemaRegistryClient(baseUrl: baseUrl)
        self.bodyEncoder = JsonKafkaRequestEncoder(auth: auth)

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

    func send(data cache: RecordSet) {
        schemaRegistry.requestSchemas(for: cache.metadata.topic) { [weak self] pair in
            guard let self = self else { return }

            guard let pair = pair else {
                self.context.didFail(for: cache.topic.name)
                return
            }
            guard let body = self.bodyEncoder.encode(data: cache, as: pair) else {
                os_log("Failed to encode data for topic %@, discarding,", cache.name)
                self.context.didSucceed(for: cache.topic.name)
                return
            }
            let url = self.baseUrl.appendingPathComponent(cache.name, isDirectory: false)
            var request = URLRequest(url: url)
            request.httpMethod = "POST"
            request.setValue(self.bodyEncoder.contentType, forHTTPHeaderField: "Content-Type")
            self.auth.addAuthorization(to: &request)

            let session: URLSession! = cache.metadata.topic.priority >= self.context.minimumPriorityForCellular ? self.highPrioritySession : self.lowPrioritySession

            let uploadTask = session.uploadTask(with: request, from: body)
            uploadTask.resume()
        }
    }

    func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive response: URLResponse, completionHandler: @escaping (URLSession.ResponseDisposition) -> Void) {

        guard let response = response as? HTTPURLResponse else { return }

        switch response.statusCode {
        case 200 ..< 300:
            context.didSucceed(metadata: records.metadata)
        case 401, 403:
            os_log("Authentication with RADAR-base failed.")
            auth.invalidate()
            context.mayRetry(cache: records)
        case 400 ..< 500:
            if let data = data, let responseBody = String(data: data, encoding: .utf8) {
                os_log("Failed code %d: %@", type: .error, response.statusCode, responseBody)
            } else {
                os_log("Failed code %d", type: .error, response.statusCode)
            }
            context.didFail(metadata: records.metadata)
            completionHandler(.cancel)
        default:
            context.serverFailure()
        }
    }

    func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
        if let error = error {
            let nsError = error as NSError
            switch nsError.code {
            case NSURLErrorInternationalRoamingOff,
                 NSURLErrorCallIsActive,
                 NSURLErrorDataNotAllowed,
                 NSURLErrorNotConnectedToInternet:
                context.couldNotConnect(metadata: records.metadata)
            default:
                context.serverFailure()
            }
            return
        }
    }

    func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
        if session == highPrioritySession, let completionHandler = highPrioritySessionCompletionHandler {
            DispatchQueue.main.async {
                completionHandler()
            }
        }
        if session == lowPrioritySession, let completionHandler = lowPrioritySessionCompletionHandler {
            DispatchQueue.main.async {
                completionHandler()
            }
        }
    }
}

protocol KafkaRequestEncoder {
    var contentType: String { get }

    func encode(data cache: RecordSet, as pair: FetchedSchemaPair) -> Data?
}

struct JsonKafkaRequestEncoder: KafkaRequestEncoder {
    static let separator = ",".data(using: .ascii)!
    static let recordKey = "{\"key\":".data(using: .ascii)!
    static let recordValue = ",\"value\":".data(using: .ascii)!
    static let recordEnd = "}".data(using: .ascii)!
    static let requestKeySchema = "{\"key_schema_id\":".data(using: .ascii)!
    static let requestValueSchema = ",\"value_schema_id\":".data(using: .ascii)!
    static let requestRecord = ",\"records\":[".data(using: .ascii)!
    static let requestEnd = "]}".data(using: .ascii)!

    let auth: Authorizer

    let contentType = "application/vnd.kafka.avro.v2+json"

    func encode(data cache: RecordSet, as pair: FetchedSchemaPair) -> Data? {
        guard let keySchema = pair.keySchema, let valueSchema = pair.valueSchema else {
            return nil
        }

        let keyData: Data
        let encoder = GenericAvroEncoder(encoding: .json)
        do {
            keyData = try encoder.encode(["projectId": auth.projectId, "userId": auth.userId, "sourceId": cache.metadata.sourceId], as: keySchema.schema)
        } catch {
            os_log("Cannot convert %@ key to schema %@", cache.name, keySchema.schema.description)
            return nil
        }

        var request = Data()
        request.append(JsonKafkaRequestEncoder.requestKeySchema)
        request.append(String(keySchema.id).data(using: .ascii)!)
        request.append(JsonKafkaRequestEncoder.requestValueSchema)
        request.append(String(valueSchema.id).data(using: .ascii)!)
        request.append(JsonKafkaRequestEncoder.requestRecord)

        var first = true
        for value in cache.values {
            guard let encodedValue = try? encoder.encode(value, as: valueSchema.schema) else {
                os_log("Cannot convert %@ value %@ to schema %@. Skipping", cache.name, value.description, valueSchema.schema.description)
                continue
            }
            if first {
                first = false
            } else {
                request.append(JsonKafkaRequestEncoder.separator)
            }
            request.append(JsonKafkaRequestEncoder.recordKey)
            request.append(keyData)
            request.append(JsonKafkaRequestEncoder.recordValue)
            request.append(encodedValue)
            request.append(JsonKafkaRequestEncoder.recordEnd)
        }
        request.append(JsonKafkaRequestEncoder.requestEnd)
        return request
    }
}
