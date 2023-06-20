//
//  AvroTopicCacheContext.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 04/09/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import BlueSteel
import CoreData
import os.log

class AvroTopicCacheContext {
    let topic: AvroTopic
    let dataGroup: NSManagedObjectID
    private let writeQueue: DispatchQueue
    private let encoder: AvroEncoder
    private let topicWriter: AvroDataWriter
    private var data: Data
    private var storeFuture: DispatchWorkItem?

    init(topic: AvroTopic, dataGroup: NSManagedObjectID, queue: DispatchQueue, encoder: AvroEncoder, topicWriter: AvroDataWriter) {
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

extension Data {
    init<T>(from value: T) {
        self = Swift.withUnsafeBytes(of: value) { Data($0) }
    }

    func load<T>(as type: T.Type) -> T {
        return withUnsafeBytes { $0.load(as: type) }
    }
}
