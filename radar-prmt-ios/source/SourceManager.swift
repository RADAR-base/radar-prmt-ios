//
//  SourceManager.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 11/12/2018.
//  Copyright © 2018 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
import CoreData
import os.log

class SourceManager {
    let topicWriter: TopicWriter
    var sourceId: String
    let encoder: AvroEncoder
    private let writeQueue: DispatchQueue
    private var dataCaches: [AvroTopicCacheContext]
    
    init?(topicWriter: TopicWriter, sourceId: String) {
        self.topicWriter = topicWriter
        self.sourceId = sourceId
        writeQueue = DispatchQueue(label: "prmt_core_data", qos: .background)
        encoder = GenericAvroEncoder(encoding: .binary)
        dataCaches = []
    }
    
    func define(topic name: String, valueSchemaPath: String, priority: Int16 = 0) -> AvroTopicCacheContext? {
        guard let path = Bundle.main.path(forResource: "radar-schemas.bundle/" + valueSchemaPath, ofType: "avsc") else {
            os_log("Schema in location radar-schemas.bundle/%@.avsc for topic %@ is not found", type: .error,
                   valueSchemaPath, name)
            return nil
        }
        guard let contents = try? String(contentsOfFile: path) else {
            os_log("Cannot read schema path %@ for topic %@", type: .error,
                   valueSchemaPath, name)
            return nil
        }

        do {
            let topic = try AvroTopic(name: name, valueSchema: contents)

            guard let dataGroup = topicWriter.register(topic: topic, sourceId: sourceId) else {
                os_log("Cannot create RecordDataGroup for topic %@", type: .error, name)
                return nil
            }

            let dataCache = AvroTopicCacheContext(topic: topic, dataGroup: dataGroup, queue: writeQueue, encoder: encoder, topicWriter: topicWriter)
            dataCaches.append(dataCache)
            return dataCache
        } catch {
            os_log("Cannot parse schema path %@ for topic %@: %@", type: .error, valueSchemaPath, name, error.localizedDescription)
            return nil
        }
    }

    func start() {
        os_log("Source %@ does not need to be started", sourceId)
    }

    func flush() {
        dataCaches.forEach { $0.flush() }
    }
}


extension Data {
    init<T>(from value: T) {
        self = Swift.withUnsafeBytes(of: value) { Data($0) }
    }
    
    func load<T>(as type: T.Type) -> T {
        return withUnsafeBytes { $0.load(as: type) }
    }
}


class AvroTopicCacheContext {
    let topic: AvroTopic
    let dataGroup: NSManagedObjectID
    private let writeQueue: DispatchQueue
    private let encoder: AvroEncoder
    private let topicWriter: TopicWriter
    private var data: Data
    private var storeFuture: DispatchWorkItem?

    init(topic: AvroTopic, dataGroup: NSManagedObjectID, queue: DispatchQueue, encoder: AvroEncoder, topicWriter: TopicWriter) {
        self.topic = topic
        self.encoder = encoder
        self.data = Data()
        self.writeQueue = queue
        self.dataGroup = dataGroup
        self.topicWriter = topicWriter
        self.storeFuture = nil
    }
    
    func add(record value: AvroValueConvertible) {
        writeQueue.async { [weak self] in
            guard let self = self else { return }
            self.encode(record: value)
            self.storeDataEventually()
        }
    }

    private func storeDataEventually() {
        if storeFuture == nil {
            storeFuture = DispatchWorkItem { [weak self] in
                guard let self = self else { return }
                self.storeData()
                self.storeFuture = nil
            }
            writeQueue.asyncAfter(deadline: .now() + 10, execute: storeFuture!)
        }
    }
    
    func flush() {
        writeQueue.sync {
            if let storeFuture = self.storeFuture {
                storeFuture.cancel()
                self.storeFuture = nil
                self.storeData()
            }
        }
    }
    
    private func storeData() {
        topicWriter.store(records: data, in: dataGroup, for: topic)
        data = Data()
    }

    private func encode(record value: AvroValueConvertible) {
        do {
            let encodedBytes = try encoder.encode(value, as: topic.valueSchema)
            data.append(Data(from: encodedBytes.count.littleEndian))
            data.append(contentsOf: encodedBytes)
        } catch {
            os_log("Cannot encode Avro value for topic %@", type: .error, topic.name)
        }
    }
}
