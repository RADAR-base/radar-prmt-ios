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

    func readNextRecords(excludingGroups exclude: [NSManagedObjectID] = [], to resultCallback: @escaping (RecordSet?) throws -> Void) {
        moc.perform { [weak self] in
            guard let self = self else { return }
            let request: NSFetchRequest<RecordDataGroup> = RecordDataGroup.fetchRequest()
            if !exclude.isEmpty {
                request.predicate = NSPredicate(format: "NOT (self IN %@)", exclude)
            }
            request.sortDescriptors = [NSSortDescriptor(key: "priority", ascending: false), NSSortDescriptor(key: "earliestTime", ascending: true)]
            request.fetchLimit = 1
            do {
                let results: [RecordDataGroup] = try self.moc.fetch(request)
                guard let dataGroup = results.first, dataGroup.earliestTime == nil || dataGroup.earliestTime! <= Date() else {
                    os_log("No stored data", type: .info)
                    try resultCallback(nil)
                    return
                }  // no groups registered
                guard let topic = try? AvroTopic(name: dataGroup.topic!, valueSchema: dataGroup.valueSchema!) else {
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

    func remove(cache: RecordSet) {
        moc.perform {
            var didSetTime = false
            for origin in cache.origins {
                let recordData = self.moc.object(with: origin.recordDataId) as! RecordData
                if let offset = origin.upToOffset {
                    os_log("Resetting offset to %d of data for topic %@ added at %{time_t}d", type: .debug,
                           offset, cache.topic.name, time_t(recordData.time!.timeIntervalSince1970))
                    recordData.offset = Int64(offset)
                    if cache.origins.count > 1 {
                        recordData.group?.earliestTime = recordData.time
                    }
                    didSetTime = true
                } else {
                    os_log("Removing sent data for topic %@", type: .debug,
                           cache.topic.name)
                    self.moc.delete(recordData)
                }
            }
            if !didSetTime {
                let group = self.moc.object(with: cache.dataGroupId) as! RecordDataGroup
                if let earliestData = group.dataset?.firstObject as? RecordData {
                    group.earliestTime = earliestData.time
                } else {
                    group.earliestTime = Date.distantFuture
                }
            }
            do {
                try self.moc.save()
            } catch {
                os_log("Failed to remove old values: %@", type: .error, error.localizedDescription)
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

    private func parseValues(from recordData: RecordData, to set: inout RecordSet, currentSize: Int) -> Int {
        var origin = RecordSet.Origin(recordDataId: recordData.objectID)
        set.origins.append(RecordSet.Origin(recordDataId: recordData.objectID))
        guard let data = recordData.data else {
            os_log("No data in RecordData entry for topic %@", type: .error, set.topic.name)
            set.origins.append(origin)
            return currentSize
        }
        var size = currentSize
        var offset = Int(recordData.offset)
        while offset < data.count, set.values.count < maxCount {
            let nextSize = data.advanced(by: offset).scan() as Int
            size += nextSize
            if set.values.count > 0 && size > maxSize {
                break
            }
            offset += MemoryLayout<Int>.size
            do {
                let avroValue = try decoder.decode(data.subdata(in: offset ..< offset + nextSize), as: set.topic.valueSchema)
                set.values.append(avroValue)
            } catch {
                os_log("Failed to decode AvroValue for topic %@", type: .error, set.topic.name)
            }
            offset += nextSize
        }

        if offset < data.count {
            origin.upToOffset = offset
        }
        set.origins.append(origin)
        return size
    }
}

struct RecordSet {
    let topic: AvroTopic
    let dataGroupId: NSManagedObjectID
    let sourceId: String
    var values: [AvroValue]
    fileprivate var origins: [Origin]

    init?(topic: AvroTopic, dataGroup: RecordDataGroup) {
        self.topic = topic
        dataGroupId = dataGroup.objectID
        guard let sourceId = dataGroup.sourceId else {
            return nil
        }
        self.sourceId = sourceId
        values = []
        origins = []
    }

    fileprivate struct Origin {
        let recordDataId: NSManagedObjectID
        var upToOffset: Int? = nil

        init(recordDataId: NSManagedObjectID) {
            self.recordDataId = recordDataId
        }
    }
}
