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
        self.moc.mergePolicy = NSMergePolicy(merge: .mergeByPropertyStoreTrumpMergePolicyType)
    }

    func store(records data: Data, in dataGroupId: NSManagedObjectID, for topic: AvroTopic) {
        moc.perform {
            os_log("Storing records for topic %@ (size %{iec-bytes}d)", type: .debug, topic.name, data.count)
            guard let object = try? self.moc.existingObject(with: dataGroupId), let dataGroup = object as? RecordDataGroup else {
                os_log("Cannot find data group for topic %@", type: .error, topic.name)
                return
            }
            let recordData = RecordData(context: self.moc)
            let now = Date()
            recordData.time = now
            recordData.data = data
            recordData.group = dataGroup
            recordData.offset = 0
            self.moc.insert(recordData)

            if dataGroup.earliestTime == nil || recordData.time! < dataGroup.earliestTime! {
                dataGroup.earliestTime = recordData.time
            }

            do {
                try self.moc.save()
                os_log("Stored records for topic %@", type: .debug, topic.name)
            } catch {
                os_log("Failed to save records for topic %@: %@", type: .error,
                       topic.name, error.localizedDescription)
            }
        }
    }
    
    func register(topic: AvroTopic, sourceId: String) -> NSManagedObjectID? {
        let fetchRequest: NSFetchRequest<RecordDataGroup> = RecordDataGroup.fetchRequest()
        fetchRequest.predicate = NSPredicate(format: "topic == %@ AND sourceId == %@ AND valueSchema == %@", topic.name, sourceId, topic.valueSchemaString)

        var result: NSManagedObjectID?
        moc.performAndWait {
            do {
                let topics = try self.moc.fetch(fetchRequest)
                let recordDataGroup: RecordDataGroup
                if topics.isEmpty {
                    recordDataGroup = RecordDataGroup(context: self.moc)
                    recordDataGroup.sourceId = sourceId
                    recordDataGroup.valueSchema = topic.valueSchemaString
                    recordDataGroup.topic = topic.name
                    recordDataGroup.earliestTime = Date.distantFuture
                    self.moc.insert(recordDataGroup)
                } else {
                    recordDataGroup = topics[0]
                }
                if topic.priority != recordDataGroup.priority {
                    recordDataGroup.priority = topic.priority
                }
                if recordDataGroup.hasChanges {
                    try self.moc.save()
                }
                result = recordDataGroup.objectID
            } catch {
                os_log("Failed to create topic topic: %@", type: .error, error.localizedDescription)
                result = nil
            }
        }
        return result
    }
}
