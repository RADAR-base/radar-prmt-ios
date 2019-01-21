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
    
    init(container: NSPersistentContainer, maxSize: Int64 = 1_000_000, maxCount: Int = 1000, margin: Double = 0.5) {
        self.moc = container.newBackgroundContext()
        self.maxSize = maxSize
        self.maxCount = maxCount
        self.margin = 1.0 - margin
    }
    
    func getNextRecords(resultCallback: @escaping (ResultCache) -> ()) {
        moc.perform {
            let request: NSFetchRequest<RecordDataGroup> = RecordDataGroup.fetchRequest()
            request.predicate = NSPredicate(format: "earliestTime > %@", Date(timeIntervalSinceReferenceDate: 0) as NSDate)
            request.sortDescriptors = [NSSortDescriptor(key: "priority", ascending: false), NSSortDescriptor(key: "earliestTime", ascending: true)]
            request.fetchLimit = 1
            do {
                let results: [RecordDataGroup] = try self.moc.fetch(request)
                guard let dataGroup = results.first else { return }  // no groups registered
                guard let topic = try? AvroTopic(name: dataGroup.topic!, valueSchema: dataGroup.valueSchema!) else {
                    os_log("Schema for topic %@ cannot be parsed: %@", type: .error, dataGroup.topic!, dataGroup.valueSchema!)
                    self.moc.delete(dataGroup)
                    try? self.moc.save()
                    return
                }
                guard let values = self.parseValues(dataGroup: dataGroup, topic: topic) else { return }
                try? self.moc.save()
                resultCallback(ResultCache(topic: topic, sourceId: dataGroup.sourceId!, values: values))
            } catch {
                os_log("Failed to get or update record data: %@", type: .fault, error.localizedDescription)
            }
        }
    }
    
    private func parseValues(dataGroup: RecordDataGroup, topic: AvroTopic) -> [AvroValue]? {
        var currentSize = 0
        var values: [AvroValue] = []
        
        guard var iter = dataGroup.dataset?.makeIterator() else {
            os_log("No dataset in RecordDataGroup entry for topic %@", type: .error, topic.name)
            return nil
        }

        let decoder = BinaryAvroDecoder()

        while currentSize < Int(ceil(Double(maxSize) * margin)),
            values.count < Int(ceil(Double(maxCount) * margin)),
            let recordData = iter.next() as? RecordData {
                guard let data = recordData.data else {
                    os_log("No data in RecordData entry for topic %@", type: .error, topic.name)
                    continue
                }
                var offset = Int(recordData.offset)
                while offset < data.count {
                    let nextSize = data.subdata(in: offset ..< MemoryLayout<Int>.size + offset).scan() as Int
                    offset += MemoryLayout<Int>.size
                    currentSize += nextSize
                    if values.count > 0 && currentSize > maxSize {
                        break
                    }
                    do {
                        let avroValue = try decoder.decode(data.subdata(in: offset ..< offset + nextSize), as: topic.valueSchema)
                        values.append(avroValue)
                    } catch {
                        // FIXME: do something
                    }
                    offset += nextSize
                }
                if offset == data.count {
                    moc.delete(recordData)
                } else {
                    recordData.offset = Int64(offset)
                }
        }
        
        if let nextData = iter.next() as? RecordData {
            dataGroup.earliestTime = nextData.time
        } else {
            dataGroup.earliestTime = 0.0
        }

        return values
    }
}

struct ResultCache {
    let topic: AvroTopic
    let sourceId: String
    let values: [AvroValue]
}
