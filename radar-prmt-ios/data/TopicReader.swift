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
    var maxSize: Int64
    var maxCount: Int
    var margin: Double
    let decoder: AvroDecoder
    static let retryFailInterval: TimeInterval = 600
    static let maxRetryInterval: TimeInterval = 86400

    init(container: NSPersistentContainer, maxSize: Int64 = 1_000_000, maxCount: Int = 1000, margin: Double = 0.5) {
        self.moc = container.newBackgroundContext()
        self.moc.mergePolicy = NSMergePolicy(merge: .mergeByPropertyStoreTrumpMergePolicyType)
        self.maxSize = maxSize
        self.maxCount = maxCount
        self.margin = 1.0 - margin
        self.decoder = BinaryAvroDecoder()
    }

    func readNextRecords(excludingGroups exclude: [String] = [], minimumPriority: Int? = nil, to resultCallback: @escaping (RecordSet?) throws -> Void) {
        moc.perform { [weak self] in
            guard let self = self else { return }
            let request: NSFetchRequest<KafkaTopic> = KafkaTopic.fetchRequest()
            var predicates: [NSPredicate] = []
            predicates.append(NSPredicate(format: "upload = NULL"))
            if !exclude.isEmpty {
                predicates.append(NSPredicate(format: "NOT (name IN %@)", exclude))
            }
            if let minimumPriority = minimumPriority {
                predicates.append(NSPredicate(format: "priority >= %@", minimumPriority))
            }
            predicates.append(NSPredicate(format: "earliestTime <= %@", Date() as NSDate))
            request.predicate = NSCompoundPredicate(andPredicateWithSubpredicates: predicates)
            request.sortDescriptors = [NSSortDescriptor(key: "priority", ascending: false), NSSortDescriptor(key: "earliestTime", ascending: true)]
            request.fetchLimit = 1

            do {
                let topics = try self.moc.fetch(request)
                guard let topic = topics.first else {
                    os_log("No stored data", type: .info)
                    try resultCallback(nil)
                    return
                }  // no groups registered
                guard let avroTopic = try? AvroTopic(name: dataGroup.topic!, valueSchema: dataGroup.valueSchema!) else {
                    os_log("Schema for topic %@ cannot be parsed: %@", type: .error, dataGroup.topic!, dataGroup.valueSchema!)
                    self.moc.delete(dataGroup)
                    try? self.moc.save()
                    return
                }
                guard let values = self.parseValues(from: dataGroup, topic: topic) else { return }
                try resultCallback(values)
            } catch {
                os_log("Failed to get or update record data: %@", type: .error, error.localizedDescription)
            }
        }
    }

    func rollbackUpload(for topic: String) {
        moc.perform {
            let request: NSFetchRequest<RecordSetUpload> = RecordSetUpload.fetchRequest()
            request.predicate = NSPredicate(format: "name = %@", topic)
            do {
                let uploads = try self.moc.fetch(request)
                for upload in uploads {
                    self.moc.delete(upload)
                }
            } catch {
                os_log("Failed to rollback upload for topic %@: %@", type: .error, topic, error.localizedDescription)
            }
        }
    }

    func removeUpload(for topic: String) {
        moc.perform {
            let request: NSFetchRequest<RecordSetUpload> = RecordSetUpload.fetchRequest()
            request.predicate = NSPredicate(format: "topic = %@", topic)
            do {
                let results: [RecordSetUpload] = try self.moc.fetch(request)
                guard !results.isEmpty else {
                    os_log("Cannot remove non-registered upload for topic %@", type: .error, topic)
                    return
                }
                for result in results {
                    var earliestTime: Date? = nil
                    for uploadPart in result.uploadPart! {
                        if let offset = uploadPart.upToOffset {
                            uploadPart.data?.offset = upToOffset
                            earliestTime = uploadPart.data?.time
                        } else {
                            self.moc.delete(uploadPart.data)
                        }
                    }
                    if let earliestTime = earliestTime {
                        result.group?.earliestTime = earliestTime
                    } else if let first = result.group?.dataset?.firstObject {
                        result.group?.earliestTime = first.earliestTime
                    } else {
                        result.group?.earliestTime = Date.distantFuture
                    }
                }

                try self.moc.save()
            } catch {
                os_log("Failed to remove old values for topic %@: %@", type: .error, topic, error.localizedDescription)
            }
        }
    }

    private func parseValues(from dataGroup: RecordDataGroup, topic: AvroTopic) -> RecordSet? {
        guard var iter = dataGroup.dataset?.makeIterator(), var recordSet = RecordSet(topic: topic, dataGroup: dataGroup) else {
            os_log("No dataset in RecordDataGroup entry for topic %@", type: .error, topic.name)
            return nil
        }
        var currentSize = 0

        while currentSize < Int(ceil(Double(maxSize) * margin)),
            recordSet.values.count < Int(ceil(Double(maxCount) * margin)),
            let recordData = iter.next() as? RecordData
        {
            currentSize = parseValues(from: recordData, to: &recordSet, currentSize: currentSize)
        }

        return recordSet
    }

    private func parseValues(from recordData: RecordData, to set: inout RecordSet, as schema: Schema, currentSize: Int) -> Int {
        var origin = RecordSet.Origin(recordDataId: recordData.objectID)
        set.metadata.origins.append(RecordSet.Origin(recordDataId: recordData.objectID))
        guard let data = recordData.data?.data else {
            os_log("No data in RecordData entry for topic %@", type: .error, set.metadata.topic.name)
            set.metadata.origins.append(origin)
            return currentSize
        }
        var size = currentSize
        var offset = Int(recordData.offset)
        while offset < data.count, set.values.count < maxCount {
            let nextSize = data.advanced(by: offset).load(as: Int.self)
            size += nextSize
            if set.values.count > 0 && size > maxSize {
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
            origin.upToOffset = offset
        }
        set.metadata.origins.append(origin)
        return size
    }
}

struct RecordSet {
    let topic: String
    let sourceId: String
    var values: [AvroValue]

    init?(topic: AvroTopic) {
        self.topic = topic.name
        values = []
    }
}

