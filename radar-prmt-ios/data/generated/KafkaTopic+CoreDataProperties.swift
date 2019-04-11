//
//  KafkaTopic+CoreDataProperties.swift
//  
//
//  Created by Joris Borgdorff on 11/04/2019.
//
//  This file was automatically generated and should not be edited.
//

import Foundation
import CoreData


extension KafkaTopic {

    @nonobjc public class func fetchRequest() -> NSFetchRequest<KafkaTopic> {
        return NSFetchRequest<KafkaTopic>(entityName: "KafkaTopic")
    }

    @NSManaged public var name: String?
    @NSManaged public var priority: Int16
    @NSManaged public var dataGroups: NSSet?
    @NSManaged public var recordSets: NSSet?
    @NSManaged public var upload: RecordSetUpload?

}

// MARK: Generated accessors for dataGroups
extension KafkaTopic {

    @objc(addDataGroupsObject:)
    @NSManaged public func addToDataGroups(_ value: RecordSetGroup)

    @objc(removeDataGroupsObject:)
    @NSManaged public func removeFromDataGroups(_ value: RecordSetGroup)

    @objc(addDataGroups:)
    @NSManaged public func addToDataGroups(_ values: NSSet)

    @objc(removeDataGroups:)
    @NSManaged public func removeFromDataGroups(_ values: NSSet)

}

// MARK: Generated accessors for recordSets
extension KafkaTopic {

    @objc(addRecordSetsObject:)
    @NSManaged public func addToRecordSets(_ value: RecordSet)

    @objc(removeRecordSetsObject:)
    @NSManaged public func removeFromRecordSets(_ value: RecordSet)

    @objc(addRecordSets:)
    @NSManaged public func addToRecordSets(_ values: NSSet)

    @objc(removeRecordSets:)
    @NSManaged public func removeFromRecordSets(_ values: NSSet)

}
