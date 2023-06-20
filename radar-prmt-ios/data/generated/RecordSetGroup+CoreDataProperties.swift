//
//  RecordSetGroup+CoreDataProperties.swift
//  
//
//  Created by Joris Borgdorff on 11/04/2019.
//
//  This file was automatically generated and should not be edited.
//

import Foundation
import CoreData

extension RecordSetGroup {

    @nonobjc public class func fetchRequest() -> NSFetchRequest<RecordSetGroup> {
        return NSFetchRequest<RecordSetGroup>(entityName: "RecordSetGroup")
    }

    @NSManaged public var sourceId: String?
    @NSManaged public var valueSchema: String?
    @NSManaged public var dataset: NSSet?
    @NSManaged public var topic: KafkaTopic?
    @NSManaged public var upload: RecordSetUpload?

}

// MARK: Generated accessors for dataset
extension RecordSetGroup {
    @objc(addDatasetObject:)
    @NSManaged public func addToDataset(_ value: RecordSet)

    @objc(removeDatasetObject:)
    @NSManaged public func removeFromDataset(_ value: RecordSet)

    @objc(addDataset:)
    @NSManaged public func addToDataset(_ values: NSSet)

    @objc(removeDataset:)
    @NSManaged public func removeFromDataset(_ values: NSSet)
}
