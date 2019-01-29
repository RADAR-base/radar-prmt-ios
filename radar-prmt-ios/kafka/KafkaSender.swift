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

class KafkaSender: NSObject, URLSessionTaskDelegate {
    let context: KafkaSendContext
    let auth: Authorizer
    var queue: OperationQueue!
    let schemaRegistry: SchemaRegistryClient
    let baseUrl: URL
    let bodyEncoder: KafkaRequestEncoder
    var minimumPriorityWithCellular = 1
    var highPrioritySession: URLSession!
    var lowPrioritySession: URLSession!

    init(baseUrl: URL, context: KafkaSendContext, auth: Authorizer) {
        var kafkaUrl = baseUrl
        kafkaUrl.appendPathComponent("kafka", isDirectory: true)
        kafkaUrl.appendPathComponent("topics", isDirectory: true)
        self.baseUrl = kafkaUrl
        self.context = context
        self.auth = auth
        self.schemaRegistry = SchemaRegistryClient(baseUrl: baseUrl)
        self.bodyEncoder = JsonKafkaRequestEncoder(auth: auth)

        let sessionConfig = URLSessionConfiguration()
        sessionConfig.waitsForConnectivity = true
        sessionConfig.allowsCellularAccess = true
        super.init()
        queue = OperationQueue()
        highPrioritySession = URLSession(configuration: sessionConfig, delegate: self, delegateQueue: queue)
        sessionConfig.allowsCellularAccess = false
        lowPrioritySession = URLSession(configuration: sessionConfig, delegate: self, delegateQueue: queue)
    }

    func send(data cache: RecordSet) {
        schemaRegistry.requestSchemas(for: cache.topic) { [weak self] pair in
            guard let self = self else { return }

            guard let pair = pair else {
                self.context.didFail(cache: cache)
                return
            }
            guard let body = self.bodyEncoder.encode(data: cache, as: pair) else {
                os_log("Failed to encode data for topic %@, discarding,", cache.topic.name)
                self.context.didSucceed(cache: cache)
                return
            }
            let url = self.baseUrl.appendingPathComponent(cache.topic.name, isDirectory: false)
            var request = URLRequest(url: url)
            request.setValue(self.bodyEncoder.contentType, forHTTPHeaderField: "Content-Type")
            self.auth.addAuthorization(to: &request)

            let session = cache.topic.priority >= self.minimumPriorityWithCellular ? self.highPrioritySession : self.lowPrioritySession

            session!.uploadTask(with: request, from: body) { [weak self] (data, response, error) in
                self?.processUpload(of: cache, data: data, response: response, error: error)
            }
        }
    }

    private func processUpload(of records: RecordSet, data: Data?, response: URLResponse?, error: Error?) {
        guard let response = response as? HTTPURLResponse else { return }

        switch response.statusCode {
        case 200 ..< 300:
            self.context.didSucceed(cache: records)
        case 401, 403:
            self.auth.invalidate()
            self.context.mayRetry(cache: records)
        case 400 ..< 500:
            if let data = data, let responseBody = String(data: data, encoding: .utf8) {
                os_log("Failed code %d: %@", type: .error, response.statusCode, responseBody)
            } else {
                os_log("Failed code %d", type: .error, response.statusCode)
            }
            self.context.didFail(cache: records)
        case 500...:
            self.context.serverFailure(cache: records)
        default:
            self.context.couldNotConnect(cache: records)
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
            keyData = try encoder.encode(["projectId": auth.projectId, "userId": auth.userId, "sourceId": cache.sourceId], as: keySchema.schema)
        } catch {
            os_log("Cannot convert %@ key to schema %@", cache.topic.name, keySchema.schema.description)
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
                os_log("Cannot convert %@ value %@ to schema %@. Skipping", cache.topic.name, value.description, valueSchema.schema.description)
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
