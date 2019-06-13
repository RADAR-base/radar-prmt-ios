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

protocol SourceManager {
    var provider: DelegatedSourceProvider { get }
    var name: String { get }
    var sourceId: String? { get set }
    func start()
    func flush()
    func close()
    func closeForeground()
}

protocol SourceProvider {
    var sourceDefinition: SourceDefinition { get }

    func provide(writer: AvroDataWriter, authConfig: RadarState) -> SourceManager?
    func matches(sourceType: SourceType) -> Bool
}

struct DelegatedSourceProvider: SourceProvider, Equatable, Hashable {
    let delegate: SourceProvider

    var sourceDefinition: SourceDefinition { return delegate.sourceDefinition }

    func provide(writer: AvroDataWriter, authConfig: RadarState) -> SourceManager? {
        return delegate.provide(writer: writer, authConfig: authConfig)
    }
    func matches(sourceType: SourceType) -> Bool {
        return delegate.matches(sourceType: sourceType)
    }

    init(_ provider: SourceProvider) {
        self.delegate = provider
    }

    static func ==(lhs: DelegatedSourceProvider, rhs: DelegatedSourceProvider) -> Bool {
        return lhs.delegate.sourceDefinition.pluginName == rhs.delegate.sourceDefinition.pluginName
    }

    func hash(into hasher: inout Hasher) {
        hasher.combine(delegate.sourceDefinition.pluginName)
    }
}

struct SourceDefinition {
    let pluginName: String
    let pluginNames: [String]
    let supportsBackground: Bool

    init(pluginNames: [String], supportsBackground: Bool = false) {
        self.pluginName = pluginNames[0]
        self.pluginNames = pluginNames
        self.supportsBackground = supportsBackground
    }
}

extension SourceDefinition: Equatable {
    static func == (lhs: SourceDefinition, rhs: SourceDefinition) -> Bool {
        return lhs.pluginName == rhs.pluginName
    }
}

typealias SourceManagerType = DataSourceManager & SourceManager

class DataSourceManager {
    let topicWriter: AvroDataWriter

    private var localSourceId: String?

    var sourceId: String? {
        get {
            return localSourceId
        }
        set(value) {
            localSourceId = value
        }
    }

    let encoder: AvroEncoder
    private let writeQueue: DispatchQueue
    private var dataCaches: [AvroTopicCacheContext]
    private let localProvider: DelegatedSourceProvider
    var provider: DelegatedSourceProvider {
        get {
            return localProvider
        }
    }

    init?(provider: DelegatedSourceProvider, topicWriter: AvroDataWriter, sourceId: String?) {
        self.localProvider = provider
        self.topicWriter = topicWriter
        self.localSourceId = sourceId
        writeQueue = DispatchQueue(label: "prmt_core_data", qos: .background)
        encoder = GenericAvroEncoder(encoding: .binary)
        dataCaches = []
    }
    
    func define(topic name: String, valueSchemaPath: String, priority: Int16 = 0) -> AvroTopicCacheContext? {
        guard let sourceId = sourceId else {
            os_log("Cannot define topic %@ without a source ID", type: .error, name)
            return nil
        }
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
        os_log("Plugin %@ does not need to be started", String(describing: self))
    }

    func flush() {
        dataCaches.forEach { $0.flush() }
    }

    final func close() {
        willClose()
        flush()
    }

    func willClose() {

    }

    final func closeForeground() {
        willCloseForeground()
        flush()
    }

    func willCloseForeground() {
        willClose()
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
