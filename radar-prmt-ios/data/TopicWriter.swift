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
            let dataContainer = DataContainer(context: self.moc)
            let now = Date()
            recordData.time = now
            recordData.data = dataContainer
            recordData.group = dataGroup
            recordData.offset = 0
            self.moc.insert(dataContainer)
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
        var result: NSManagedObjectID?
        moc.performAndWait {
            let topicRequest: NSFetchRequest<KafkaTopic> = KafkaTopic.fetchRequest()
            topicRequest.predicate = NSPredicate(format: "topic == %@")

            do {
                let topics = try self.moc.fetch(topicRequest)

                let kafkaTopic: KafkaTopic
                if topics.isEmpty {
                    kafkaTopic = KafkaTopic(context: self.moc)
                    kafkaTopic.name = topic.name
                    kafkaTopic.earliestTime = Date.distantFuture
                    self.moc.insert(kafkaTopic)
                } else {
                    kafkaTopic = topics[0]
                }
                if kafkaTopic.priority != topic.priority {
                    kafkaTopic.priority = topic.priority
                }

                let recordDataGroup: RecordDataGroup
                if let group = (kafkaTopic.dataGroups as? Set<RecordDataGroup>)?.first(where: { g in
                    g.valueSchema == topic.valueSchemaString && g.sourceId == sourceId
                }) {
                    recordDataGroup = group
                } else {
                    recordDataGroup = RecordDataGroup(context: self.moc)
                    recordDataGroup.topic = kafkaTopic
                    recordDataGroup.valueSchema = topic.valueSchemaString
                    recordDataGroup.sourceId = sourceId
                    recordDataGroup.earliestTime = Date.distantFuture
                    self.moc.insert(recordDataGroup)
                }

                if kafkaTopic.hasChanges || recordDataGroup.hasChanges {
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
