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

class AvroDataWriter {
    let moc: NSManagedObjectContext
    
    init(container: NSPersistentContainer) {
        self.moc = container.newBackgroundContext()
        self.moc.mergePolicy = NSMergePolicy.mergeByPropertyStoreTrump
    }

    func store(records data: Data, in dataGroupId: NSManagedObjectID, for topic: AvroTopic) {
        moc.perform {
            os_log("Storing records for topic %@ (size %{iec-bytes}d)", type: .debug, topic.name, data.count)
            guard let object = try? self.moc.existingObject(with: dataGroupId), let group = object as? RecordSetGroup else {
                os_log("Cannot find data group for topic %@", type: .error, topic.name)
                return
            }
            let dataContainer = DataContainer(entityContext: self.moc)
            dataContainer.data = data
            self.moc.insert(dataContainer)
            let recordData = RecordSet(entityContext: self.moc)
            recordData.time = Date()
            recordData.dataContainer = dataContainer
            recordData.group = group
            recordData.offset = 0
            recordData.topic = group.topic
            self.moc.insert(recordData)

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
            topicRequest.predicate = NSPredicate(format: "name == %@", topic.name)
            topicRequest.relationshipKeyPathsForPrefetching = ["dataGroups"]

            do {
                let topics = try self.moc.fetch(topicRequest)

                let kafkaTopic: KafkaTopic
                if topics.isEmpty {
                    kafkaTopic = KafkaTopic(entityContext: self.moc)
                    kafkaTopic.name = topic.name
                    self.moc.insert(kafkaTopic)
                } else {
                    kafkaTopic = topics[0]
                }
                if kafkaTopic.priority != topic.priority {
                    kafkaTopic.priority = topic.priority
                }

                let recordDataGroup: RecordSetGroup
                if let group = (kafkaTopic.dataGroups as? Set<RecordSetGroup>)?.first(where: { g in
                    g.valueSchema == topic.valueSchemaString && g.sourceId == sourceId
                }) {
                    recordDataGroup = group
                } else {
                    recordDataGroup = RecordSetGroup(entityContext: self.moc)
                    recordDataGroup.topic = kafkaTopic
                    recordDataGroup.valueSchema = topic.valueSchemaString
                    recordDataGroup.sourceId = sourceId
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
