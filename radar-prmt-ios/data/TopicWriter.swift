//
//  TopicWriter.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 10/12/2018.
//  Copyright © 2018 Joris Borgdorff. All rights reserved.
//

import Foundation
import CoreData
import os.log

class TopicWriter {
    let moc: NSManagedObjectContext
    
    init(container: NSPersistentContainer) {
        self.moc = container.newBackgroundContext()
    }

    func storeRecords(topic: AvroTopic, dataGroupId: NSManagedObjectID, data: Data) {
        moc.perform {
            guard let dataGroup = try? self.moc.existingObject(with: dataGroupId) as! RecordDataGroup else {
                os_log("Cannot find data group for topic %@", topic.name)
                return
            }
            let recordData = RecordData(context: self.moc)
            recordData.time = Date().timeIntervalSinceReferenceDate
            recordData.data = data
            recordData.group = dataGroup
            recordData.offset = 0
            recordData.count = Int64(data.count)
            self.moc.insert(recordData)

            if dataGroup.earliestTime != 0 || recordData.time < dataGroup.earliestTime {
                dataGroup.earliestTime = recordData.time
            }

            do {
                try self.moc.save()
            } catch {
                assertionFailure("Failed to save records for topic \(topic.name): \(error)")
            }
        }
    }
    
    func registerRecordGroup(topic: AvroTopic, sourceId: String, callback: (NSManagedObjectID) -> ()) {
        let fetchRequest: NSFetchRequest<RecordDataGroup> = RecordDataGroup.fetchRequest()
        fetchRequest.predicate = NSPredicate(format: "topic == %@ AND sourceId == %@ AND valueSchema == %@", topic.name, sourceId, topic.valueSchemaString)

        moc.performAndWait {
            do {
                let topics = try self.moc.fetch(fetchRequest)
                let recordDataGroup: RecordDataGroup
                if topics.isEmpty {
                    recordDataGroup = RecordDataGroup(context: self.moc)
                    recordDataGroup.sourceId = sourceId
                    recordDataGroup.valueSchema = topic.valueSchemaString
                    recordDataGroup.topic = topic.name
                    self.moc.insert(recordDataGroup)
                } else {
                    recordDataGroup = topics[0]
                }
                recordDataGroup.priority = topic.priority
                if recordDataGroup.hasChanges {
                    try self.moc.save()
                }
                callback(recordDataGroup.objectID)
            } catch {
                fatalError("Failed to fetch topics: \(error)")
            }
        }
    }
}
