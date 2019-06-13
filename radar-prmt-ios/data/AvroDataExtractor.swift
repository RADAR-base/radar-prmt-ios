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
import RxSwift

class AvroDataExtractor {
    let moc: NSManagedObjectContext
    var config: AvroDataExtractionConfiguration
    let decoder: AvroDecoder

    init(container: NSPersistentContainer) {
        self.moc = container.newBackgroundContext()
        self.moc.mergePolicy = NSMergePolicy.mergeByPropertyObjectTrump
        self.config = AvroDataExtractionConfiguration()
        self.decoder = BinaryAvroDecoder()
    }

    /**
     Read a set of next records from the data store. Data will only be read from a single Kafka topic.
     */
    func readRecords(from dataGroupId: NSManagedObjectID, with context: UploadContext, schemas: (SchemaMetadata, SchemaMetadata)) -> Observable<(UploadHandle, Bool)> {
        return Single.create { [weak self] observable in
            self?.moc.perform { [weak self] in
                guard let self = self else { return }

                do {
                    guard let dataGroup = try self.moc.existingObject(with: dataGroupId) as? RecordSetGroup else {
                        os_log("No stored data", type: .info)
                        observable(.error(AvroDataExtractionError.noData))
                        return
                    }  // no groups registered
                    os_log("Parsing next data group for topic %@", dataGroup.topic!.name!)
                    guard let topic = dataGroup.topic,
                        let topicName = topic.name,
                        let valueSchema = dataGroup.valueSchema else {
                            os_log("Invalid data group")
                            self.moc.delete(dataGroup)
                            try? self.moc.save()
                            observable(.error(AvroDataExtractionError.decodingError))
                            return
                    }

                    guard let avroTopic = try? AvroTopic(name: topicName, valueSchema: valueSchema) else {
                        os_log("Schema for topic %@ cannot be parsed: %@", type: .error, topicName, valueSchema)
                        self.moc.delete(dataGroup)
                        try? self.moc.save()
                        observable(.error(AvroDataExtractionError.decodingError))
                        return
                    }

                    let (handle, hasMore) = try self.prepareUpload(fromGroup: dataGroup, topic: avroTopic, with: context, schemas: schemas)

                    observable(.success((handle, hasMore)))
                } catch {
                    os_log("Failed to get or update record data: %@", type: .error, error.localizedDescription)
                    observable(.error(error))
                }
            }
            return Disposables.create()
        }.asObservable()
    }

    func nextInQueue(minimumPriority: Int?) -> Observable<UploadQueueElement> {
        return Observable.create { [weak self] observable in
            self?.moc.perform { [weak self] in
                guard let self = self else { return }
                do {
                    let request: NSFetchRequest<RecordSetGroup> = RecordSetGroup.fetchRequest()
                    var predicates: [NSPredicate] = []
                    predicates.append(NSPredicate(format: "topic.upload == NULL"))
                    if let minimumPriority = minimumPriority {
                        predicates.append(NSPredicate(format: "topic.priority >= %d", minimumPriority))
                    }
                    predicates.append(NSPredicate(format: "dataset.@count > 0"))
                    request.predicate = NSCompoundPredicate(andPredicateWithSubpredicates: predicates)
                    request.sortDescriptors = [NSSortDescriptor(key: "topic.priority", ascending: false)]
                    request.relationshipKeyPathsForPrefetching = ["topic"]

                    var topics = Set<String>()
                    for group in try self.moc.fetch(request) {
                        if let topic = group.topic, let topicName = topic.name, !topics.contains(topicName) {
                            observable.onNext(.fresh(topic: topicName, dataGroupId: group.objectID))
                            topics.insert(topicName)
                        }
                    }
                    observable.onCompleted()
                } catch {
                    os_log("Failed to get record data: %@", type: .error, error.localizedDescription)
                    observable.onError(error)
                }
            }
            return Disposables.create()
        }
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
                    upload.retryAt = Date().timeIntervalSinceReferenceDate
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
                    let newInterval = (100 ..< self.config.maxRetryInterval).exponentialBackOff(from: oldInterval, startingAt: self.config.retryFailInterval)

                    upload.retryInterval = newInterval.interval
                    upload.retryAt = Date(timeIntervalSinceNow: newInterval.backOff).timeIntervalSinceReferenceDate
                }
            } catch {
                os_log("Failed to mark old values for topic %@ as failed: %@", type: .error, topic, error.localizedDescription)
            }
        }
    }

    func nextRetry(minimumPriority: Int?) -> Observable<UploadQueueElement> {
        return Observable.create { [weak self] observable in
            self?.moc.perform { [weak self] in
                guard let self = self else { return }
                let request: NSFetchRequest<RecordSetUpload> = RecordSetUpload.fetchRequest()
                var predicates: [NSPredicate] = []
                predicates.append(NSPredicate(format: "retryAt <= %@", Date() as NSDate))
                if let minimumPriority = minimumPriority {
                    predicates.append(NSPredicate(format: "topic.priority >= %d", minimumPriority))
                }
                request.predicate = NSCompoundPredicate(andPredicateWithSubpredicates: predicates)
                request.sortDescriptors = [NSSortDescriptor(key: "retryAt", ascending: true)]
                request.relationshipKeyPathsForPrefetching = ["topic"]

                do {
                    let uploads = try self.moc.fetch(request)
                    for upload in uploads {
                        if let topic = upload.topic, let topicName = topic.name {
                            observable.onNext(.retry(topic: topicName, uploadId: upload.objectID))
                        } else {
                            os_log("Incomplete upload stored", type: .error)
                        }
                    }
                    observable.onCompleted()
                } catch {
                    os_log("Failed to retrieve failed records: %@", type: .error, error.localizedDescription)
                    observable.onError(error)
                }
            } ?? observable.onCompleted()
            return Disposables.create()
        }
    }

    func prepareUpload(for element: UploadQueueElement, with context: UploadContext, schemas: (SchemaMetadata, SchemaMetadata)) -> Observable<(UploadHandle, Bool)> {
        switch element {
        case let .fresh(topic: _, dataGroupId: dataGroupId):
            return readRecords(from: dataGroupId, with: context, schemas: schemas)
        case let .retry(topic: _, uploadId: uploadId):
            return retryUpload(for: uploadId, with: context, schemas: schemas)
        }
    }

    func retryUpload(for uploadId: NSManagedObjectID, with context: UploadContext, schemas: (SchemaMetadata, SchemaMetadata)) -> Observable<(UploadHandle, Bool)> {
        return Single.create { [weak self] observable in
            self?.moc.perform { [weak self] in
                guard let self = self else { return }

                do {
                    guard let upload = try self.moc.existingObject(with: uploadId) as? RecordSetUpload else {
                        os_log("Failed to retrieve upload")
                        observable(.error(AvroDataExtractionError.noData))
                        return
                    }

                    guard let topic = try? AvroTopic(name: upload.topic!.name!, valueSchema: upload.dataGroup!.valueSchema!) else { return }

                    let uploadHandle = try context.start(element: .retry(topic: topic.name, uploadId: uploadId), upload: upload, schemas: schemas)

                    if uploadHandle.isComplete {
                        observable(.success((uploadHandle, false)))
                        return
                    }

                    upload.retryAt = Date(timeIntervalSinceNow: self.config.assumeFailedInterval).timeIntervalSinceReferenceDate
                    try self.moc.save()

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
                                try uploadHandle.add(value: avroValue)
                            } catch {
                                os_log("Failed to decode AvroValue for topic %@", type: .error, topic.name)
                            }
                            offset += nextSize
                        }
                    }

                    try uploadHandle.finalize()
                    observable(.success((uploadHandle, false)))
                } catch {
                    os_log("Failed to reset failed topics: %@", type: .error, error.localizedDescription)
                }
            }
            return Disposables.create()
        }.asObservable()
    }

    func removeUpload(for topic: String, storedOn medium: RequestMedium) {
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
                    try medium.remove(upload: upload)

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

    private func prepareUpload(fromGroup dataGroup: RecordSetGroup, topic: AvroTopic, with context: UploadContext, schemas: (SchemaMetadata, SchemaMetadata)) throws -> (UploadHandle, Bool) {
        let request: NSFetchRequest<RecordSet> = RecordSet.fetchRequest()
        request.predicate = NSPredicate(format: "group == %@", dataGroup)
        request.sortDescriptors = [NSSortDescriptor(key: "time", ascending: true)]
        request.fetchLimit = config.maxCount

        let uploadSet = RecordSetUpload(entityContext: moc)
        uploadSet.retryAt = Date(timeIntervalSinceNow: config.assumeFailedInterval).timeIntervalSinceReferenceDate
        uploadSet.retryInterval = 0
        uploadSet.statusCode = 0
        uploadSet.statusMessage = nil
        uploadSet.modifiedAt = Date().timeIntervalSinceReferenceDate
        uploadSet.dataGroup = dataGroup
        uploadSet.topic = dataGroup.topic

        let uploadHandle = try context.start(element: .fresh(topic: topic.name, dataGroupId: dataGroup.objectID), upload: uploadSet, schemas: schemas)
        assert(!uploadHandle.isComplete)

        moc.insert(uploadSet)

        var currentSize = 0
        let records = try moc.fetch(request)
        os_log("Parsing %d record sets for topic %@", type: .debug, records.count, topic.name)
        var hasMore = false
        for recordData in records {
            if currentSize >= config.sizeMargin() || uploadHandle.count >= config.countMargin() {
                os_log("Current size %d or count %d limit is reached.", type: .debug, currentSize, uploadHandle.count)
                hasMore = true
                break
            }
            currentSize = try prepareUploadPart(fromSet: recordData, with: uploadHandle, as: topic.valueSchema, uploadSet: uploadSet, currentSize: currentSize)
        }

        try moc.save()

        try uploadHandle.finalize()
        return (uploadHandle, hasMore)
    }

    private func prepareUploadPart(fromSet recordData: RecordSet, with context: UploadHandle, as schema: Schema, uploadSet: RecordSetUpload, currentSize: Int) throws -> Int {
        guard let container = recordData.dataContainer, let data = container.data else {
            os_log("No data in RecordData entry for topic %@", type: .error, uploadSet.topic!.name!)
            return currentSize
        }

        let uploadPart = UploadPart(entityContext: self.moc)
        uploadPart.data = recordData
        uploadPart.upload = uploadSet

        var size = currentSize
        var offset = Int(recordData.offset)
        while offset < data.count, context.count < config.maxCount {
            let nextSize = data.advanced(by: offset).load(as: Int.self)
            size += nextSize
            if context.count > 0 && size > config.maxSize {
                break
            }
            offset += MemoryLayout<Int>.size
            do {
                let avroValue = try decoder.decode(data.subdata(in: offset ..< offset + nextSize), as: schema)
                try context.add(value: avroValue)
            } catch {
                os_log("Failed to decode AvroValue for topic %@", type: .error, uploadSet.topic!.name!)
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
}

enum AvroDataExtractionError: Error {
    case noData
    case decodingError
}

extension Range where Bound == TimeInterval {
    func exponentialBackOff(from interval: TimeInterval?, startingAt defaultInterval: TimeInterval) -> (interval: TimeInterval, backOff: TimeInterval) {
        let nextInterval: TimeInterval
        if let interval = interval, interval <= self.upperBound {
            nextInterval = interval * 2
        } else {
            nextInterval = defaultInterval
        }
        return (nextInterval, Double.random(in: lowerBound ..< Swift.min(nextInterval, upperBound)))
    }
}

enum UploadQueueElement {
    case fresh(topic: String, dataGroupId: NSManagedObjectID)
    case retry(topic: String, uploadId: NSManagedObjectID)

    var topic: String {
        get {
            switch self {
            case let .fresh(topic: topic, dataGroupId: _):
                return topic
            case let .retry(topic: topic, uploadId: _):
                return topic
            }
        }
    }
}

struct AvroDataExtractionConfiguration: Equatable {
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
