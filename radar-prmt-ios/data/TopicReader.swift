//
//  TopicReader.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 10/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import CoreData
import BlueSteel
import os.log

class TopicReader {
    let moc: NSManagedObjectContext
    var config: TopicReaderConfiguration
    let decoder: AvroDecoder

    init(container: NSPersistentContainer) {
        self.moc = container.newBackgroundContext()
        self.config = TopicReaderConfiguration()
        self.decoder = BinaryAvroDecoder()
    }

    /**
     Read a set of next records from the data store. Data will only be read from a single Kafka topic.
     */
    func readNextRecords(minimumPriority: Int? = nil, to resultCallback: @escaping (RecordSetValue?) throws -> Void) {
        moc.perform { [weak self] in
            guard let self = self else { return }

            do {
                guard let dataGroup = try self.nextInQueue(minimumPriority: minimumPriority) else {
                    os_log("No stored data", type: .info)
                    try resultCallback(nil)
                    return
                }  // no groups registered
                os_log("Parsing next data group for topic %@", dataGroup.topic!.name!)
                guard let avroTopic = try? AvroTopic(name: dataGroup.topic!.name!, valueSchema: dataGroup.valueSchema!) else {
                    os_log("Schema for topic %@ cannot be parsed: %@", type: .error, dataGroup.topic!.name!, dataGroup.valueSchema!)
                    self.moc.delete(dataGroup)
                    try? self.moc.save()
                    return
                }
                guard let values = try self.parseValues(from: dataGroup, topic: avroTopic) else { return }
                try resultCallback(values)
            } catch {
                os_log("Failed to get or update record data: %@", type: .error, error.localizedDescription)
            }
        }
    }

    internal func nextInQueue(minimumPriority: Int?) throws -> RecordSetGroup? {
        let request: NSFetchRequest<RecordSet> = RecordSet.fetchRequest()
        var predicates: [NSPredicate] = []
        predicates.append(NSPredicate(format: "topic.upload == NULL"))
        if let minimumPriority = minimumPriority {
            predicates.append(NSPredicate(format: "topic.priority >= %d", minimumPriority))
        }
        request.predicate = NSCompoundPredicate(andPredicateWithSubpredicates: predicates)
        request.sortDescriptors = [NSSortDescriptor(key: "topic.priority", ascending: false),
                                   NSSortDescriptor(key: "time", ascending: true)]
        request.fetchLimit = 1

        return try moc.fetch(request).first?.group
    }

    func rollbackUpload(for topic: String) {
        moc.perform { [weak self] in
            guard let self = self else { return }
            let request: NSFetchRequest<RecordSetUpload> = RecordSetUpload.fetchRequest()
            request.predicate = NSPredicate(format: "topic.name == %@", topic)
            do {
                let uploads = try self.moc.fetch(request)
                guard !uploads.isEmpty else { return }
                for upload in uploads {
                    self.moc.delete(upload)
                }
                try self.moc.save()
            } catch {
                os_log("Failed to rollback upload for topic %@: %@", type: .error, topic, error.localizedDescription)
            }
        }
    }

    func registerUploadError(for topic: String, code: Int16, message: String) {
        moc.perform { [weak self] in
            guard let self = self else { return }

            let request: NSFetchRequest<RecordSetUpload> = RecordSetUpload.fetchRequest()
            request.predicate = NSPredicate(format: "topic.name == %@", topic)
            do {
                let uploads: [RecordSetUpload] = try self.moc.fetch(request)
                guard !uploads.isEmpty else {
                    os_log("Cannot mark non-registered upload for topic %@", type: .error, topic)
                    return
                }
                for upload: RecordSetUpload in uploads {
                    if upload.firstFailure == nil {
                        upload.firstFailure = Date()
                    }
                    upload.statusCode = code
                    upload.statusMessage = message

                    let oldInterval: TimeInterval? = upload.retryInterval == 0 ? nil : upload.retryInterval
                    let (newInterval, intervalToNextDate) = TopicReader.exponentialBackOff(from: oldInterval, startingAt: self.config.retryFailInterval, ranging: 100 ..< self.config.maxRetryInterval)

                    upload.retryInterval = newInterval
                    upload.retryAt = Date(timeIntervalSinceNow: intervalToNextDate).timeIntervalSinceReferenceDate
                }
            } catch {
                os_log("Failed to mark old values for topic %@ as failed: %@", type: .error, topic, error.localizedDescription)
            }
        }
    }

    func retryUpload(to resultCallback: @escaping (RecordSetValue?) throws -> Void) {
        moc.perform { [weak self] in
            guard let self = self else { return }

            let request: NSFetchRequest<RecordSetUpload> = RecordSetUpload.fetchRequest()
            request.predicate = NSPredicate(format: "retryAt <= %@", Date() as NSDate)
            request.sortDescriptors = [NSSortDescriptor(key: "retryAt", ascending: true)]
            request.fetchLimit = 1

            do {
                let uploads = try self.moc.fetch(request)
                guard let upload = uploads.first else {
                    try resultCallback(nil)
                    return
                }

                upload.retryAt = Date(timeIntervalSinceNow: self.config.assumeFailedInterval).timeIntervalSinceReferenceDate
                try self.moc.save()

                guard let topic = try? AvroTopic(name: upload.topic!.name!, valueSchema: upload.dataGroup!.valueSchema!) else { return }

                var value = RecordSetValue(topic: topic, sourceId: upload.dataGroup!.sourceId!)

                let partRequest: NSFetchRequest<UploadPart> = UploadPart.fetchRequest()
                partRequest.predicate = NSPredicate(format: "upload == %@", upload)
                partRequest.sortDescriptors = [NSSortDescriptor(key: "data.time", ascending: true)]
                let uploadParts = try self.moc.fetch(partRequest)

                for uploadPart in uploadParts {
                    guard let recordSet = uploadPart.data else { return }
                    var offset = Int(recordSet.offset)
                    let data = recordSet.dataContainer!.data!
                    let upToOffset = uploadPart.upToOffset == 0 ? data.count : Int(uploadPart.upToOffset)
                    while offset < upToOffset {
                        let nextSize = data.advanced(by: offset).load(as: Int.self)
                        offset += MemoryLayout<Int>.size
                        do {
                            let avroValue = try self.decoder.decode(data.subdata(in: offset ..< offset + nextSize), as: topic.valueSchema)
                            value.values.append(avroValue)
                        } catch {
                            os_log("Failed to decode AvroValue for topic %@", type: .error, value.topic)
                        }
                        offset += nextSize
                    }
                }

                try resultCallback(value)
            } catch {
                os_log("Failed to reset failed topics: %@", type: .error, error.localizedDescription)
            }
        }
    }

    func removeUpload(for topic: String) {
        moc.perform { [weak self] in
            guard let self = self else { return }
            let request: NSFetchRequest<RecordSetUpload> = RecordSetUpload.fetchRequest()
            request.predicate = NSPredicate(format: "topic.name == %@", topic)
            do {
                let uploads = try self.moc.fetch(request)
                guard !uploads.isEmpty else {
                    os_log("Cannot remove non-registered upload for topic %@", type: .error, topic)
                    return
                }
                for upload in uploads {
                    for uploadPart in upload.origins! {
                        guard let uploadPart: UploadPart = uploadPart as? UploadPart else {
                            os_log("Upload part not specified correctly %@", type: .error, topic)
                            return
                        }
                        if uploadPart.upToOffset > 0 {
                            uploadPart.data?.offset = uploadPart.upToOffset
                        } else {
                            self.moc.delete(uploadPart.data!)
                        }
                    }
                    self.moc.delete(upload)
                }

                try self.moc.save()
            } catch {
                os_log("Failed to remove old values for topic %@: %@", type: .error, topic, error.localizedDescription)
            }
        }
    }

    private func parseValues(from dataGroup: RecordSetGroup, topic: AvroTopic) throws -> RecordSetValue? {
        let request: NSFetchRequest<RecordSet> = RecordSet.fetchRequest()
        request.predicate = NSPredicate(format: "group == %@", dataGroup)
        request.sortDescriptors = [NSSortDescriptor(key: "time", ascending: true)]
        request.fetchLimit = config.maxCount

        guard let sourceId = dataGroup.sourceId else {
            os_log("No dataset in RecordDataGroup entry for topic %@", type: .error, topic.name)
            return nil
        }

        var recordSet = RecordSetValue(topic: topic, sourceId: sourceId)

        let uploadSet = RecordSetUpload(entityContext: moc)
        uploadSet.retryAt = Date(timeIntervalSinceNow: config.assumeFailedInterval).timeIntervalSinceReferenceDate
        uploadSet.retryInterval = 0
        uploadSet.statusCode = 0
        uploadSet.statusMessage = nil
        uploadSet.modifiedAt = Date().timeIntervalSinceReferenceDate
        uploadSet.dataGroup = dataGroup
        uploadSet.topic = dataGroup.topic

        moc.insert(uploadSet)

        var currentSize = 0
        let records = try moc.fetch(request)
        os_log("Parsing %d record sets for topic %@", type: .debug, records.count, topic.name)
        for recordData in records {
            if currentSize >= config.sizeMargin() || recordSet.values.count >= config.countMargin() {
                os_log("Current size %d or count %d limit is reached.", type: .debug, currentSize, recordSet.values.count)
                break
            }
            currentSize = try parseValues(from: recordData, to: &recordSet, as: topic.valueSchema, uploadSet: uploadSet, currentSize: currentSize)
        }

        try moc.save()

        return recordSet
    }

    private func parseValues(from recordData: RecordSet, to set: inout RecordSetValue, as schema: Schema, uploadSet: RecordSetUpload, currentSize: Int) throws -> Int {
        guard let container = recordData.dataContainer, let data = container.data else {
            os_log("No data in RecordData entry for topic %@", type: .error, set.topic)
            return currentSize
        }

        let uploadPart = UploadPart(entityContext: self.moc)
        uploadPart.data = recordData
        uploadPart.upload = uploadSet

        var size = currentSize
        var offset = Int(recordData.offset)
        while offset < data.count, set.values.count < config.maxCount {
            let nextSize = data.advanced(by: offset).load(as: Int.self)
            size += nextSize
            if set.values.count > 0 && size > config.maxSize {
                break
            }
            offset += MemoryLayout<Int>.size
            do {
                let avroValue = try decoder.decode(data.subdata(in: offset ..< offset + nextSize), as: schema)
                set.values.append(avroValue)
            } catch {
                os_log("Failed to decode AvroValue for topic %@", type: .error, set.topic)
            }
            offset += nextSize
        }

        if offset < data.count {
            uploadPart.upToOffset = Int64(offset)
        } else {
            uploadPart.upToOffset = 0
        }
        moc.insert(uploadPart)
        uploadSet.addToOrigins(uploadPart)

        return size
    }

    private static func exponentialBackOff(from interval: TimeInterval?, startingAt defaultInterval: TimeInterval, ranging range: Range<TimeInterval>) -> (TimeInterval, TimeInterval) {
        let nextInterval: TimeInterval
        if let interval = interval, interval <= range.upperBound {
            nextInterval = interval * 2
        } else {
            nextInterval = defaultInterval
        }
        return (nextInterval, Double.random(in: range.lowerBound ..< min(nextInterval, range.upperBound)))
    }
}

struct TopicReaderConfiguration {
    var maxSize: Int64 = 1_000_000
    var maxCount: Int = 1000
    var margin: Double = 0.5
    var retryFailInterval: TimeInterval = 600
    var maxRetryInterval: TimeInterval = 86400
    var assumeFailedInterval: TimeInterval = 86400 * 3

    func sizeMargin() -> Int {
        return Int(ceil(Double(maxSize) * (1.0 - margin)))
    }

    func countMargin() -> Int {
        return Int(ceil(Double(maxCount) * (1.0 - margin)))
    }
}

struct RecordSetValue {
let topic: String
    let priority: Int16
    let sourceId: String
    var values: [AvroValue]

    init(topic: AvroTopic, sourceId: String) {
        self.topic = topic.name
        self.priority = topic.priority
        self.sourceId = sourceId
        values = []
    }
}

