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
        self.encoder = GenericAvroEncoder(encoding: .binary)
        self.dataCaches = []
    }
    
    func createTopic(name: String, valueSchemaPath: String, priority: Int16 = 0) -> AvroTopicCacheContext? {
        guard let path = Bundle.main.path(forResource: "radar-schemas.bundle/" + valueSchemaPath, ofType: "avsc") else {
            os_log("Schema in location radar-schemas.bundle/%@.avsc for topic %@ is not found", type: .error, valueSchemaPath, name)
            return nil
        }
        guard let contents = try? String(contentsOfFile: path) else {
            os_log("Cannot read schema path %@ for topic %@", type: .error, valueSchemaPath, name)
            return nil
        }

        do {
            let topic = try AvroTopic(name: "test", valueSchema: contents)

            var dataGroup: NSManagedObjectID? = nil
            topicWriter.registerRecordGroup(topic: topic, sourceId: sourceId)  { dataGroup = $0 }

            guard let initDataGroup = dataGroup else {
                os_log("Cannot create RecordDataGroup for topic %@", type: .error, name)
                return nil
            }

            let dataCache = AvroTopicCacheContext(topic: topic, dataGroup: initDataGroup, queue: writeQueue, encoder: encoder, topicWriter: topicWriter)
            dataCaches.append(dataCache)
            return dataCache
        } catch {
            os_log("Cannot parse schema path %@ for topic %@: %@", type: .error, valueSchemaPath, name, error.localizedDescription)
            return nil
        }
    }

    func flush() {
        dataCaches.forEach { $0.flush() }
    }
}


extension Data {
    init<T>(from value: T) {
        self = Swift.withUnsafeBytes(of: value) { Data($0) }
    }
    
    func scan<T>() -> T {
        return withUnsafeBytes { $0.pointee }
    }
}


class AvroTopicCacheContext {
    let topic: AvroTopic
    let dataGroup: NSManagedObjectID
    private let writeQueue: DispatchQueue
    private let encoder: AvroEncoder
    private let topicWriter: TopicWriter
    private var data: Data
    private var hasFuture: Bool

    init(topic: AvroTopic, dataGroup: NSManagedObjectID, queue: DispatchQueue, encoder: AvroEncoder, topicWriter: TopicWriter) {
        self.topic = topic
        self.encoder = encoder
        self.data = Data()
        self.writeQueue = queue
        self.dataGroup = dataGroup
        self.topicWriter = topicWriter
        self.hasFuture = false
    }
    
    func addRecord(_ value: AvroValueConvertible) {
        do {
            let avroValue = try AvroValue(value: value, as: topic.valueSchema)
            writeQueue.async {
                self.encodeRecord(avroValue)

                if (!self.hasFuture) {
                    self.hasFuture = true
                    self.writeQueue.asyncAfter(deadline: .now() + 10) { [weak self] in
                        guard let strongSelf = self else { return }
                        strongSelf.storeCache()
                    }
                }
            }
        } catch {
            os_log("Failed to convert Avro value %@", value.toAvro().description)
        }
    }
    
    func flush() {
        writeQueue.sync {
            self.storeCache()
        }
    }
    
    private func storeCache() {
        if (hasFuture) {
            topicWriter.storeRecords(topic: topic, dataGroupId: dataGroup, data: data)
            data = Data()
            hasFuture = false
        }
    }

    private func encodeRecord(_ value: AvroValue) {
        do {
            let encodedBytes = try encoder.encode(value, as: topic.valueSchema)
            data.append(Data(from: encodedBytes.count.littleEndian))
            data.append(contentsOf: encodedBytes)
        } catch {
            os_log("Cannot encode Avro value for topic %@", topic.name)
        }
    }
}

enum CoreDataError: Error {
    case cannotCreate
}

enum SchemaError: Error {
    case notFound
    case invalid
    case notReadable
}
