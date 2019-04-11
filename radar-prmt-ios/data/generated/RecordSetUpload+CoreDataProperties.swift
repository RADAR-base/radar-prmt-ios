//
//  RecordSetUpload+CoreDataProperties.swift
//  
//
//  Created by Joris Borgdorff on 11/04/2019.
//
//  This file was automatically generated and should not be edited.
//

import Foundation
import CoreData


extension RecordSetUpload {

    @nonobjc public class func fetchRequest() -> NSFetchRequest<RecordSetUpload> {
        return NSFetchRequest<RecordSetUpload>(entityName: "RecordSetUpload")
    }

    @NSManaged public var firstFailure: Date?
    @NSManaged public var modifiedAt: TimeInterval
    @NSManaged public var retryAt: TimeInterval
    @NSManaged public var retryInterval: TimeInterval
    @NSManaged public var statusCode: Int16
    @NSManaged public var statusMessage: String?
    @NSManaged public var dataGroup: RecordSetGroup?
    @NSManaged public var origins: NSOrderedSet?
    @NSManaged public var topic: KafkaTopic?

}

// MARK: Generated accessors for origins
extension RecordSetUpload {

    @objc(addOriginsObject:)
    @NSManaged public func addToOrigins(_ value: UploadPart)

    @objc(removeOriginsObject:)
    @NSManaged public func removeFromOrigins(_ value: UploadPart)

    @objc(addOrigins:)
    @NSManaged public func addToOrigins(_ values: NSOrderedSet)

    @objc(removeOrigins:)
    @NSManaged public func removeFromOrigins(_ values: NSOrderedSet)

}
