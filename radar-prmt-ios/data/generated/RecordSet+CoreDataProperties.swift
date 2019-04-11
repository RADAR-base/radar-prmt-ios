//
//  RecordSet+CoreDataProperties.swift
//  
//
//  Created by Joris Borgdorff on 11/04/2019.
//
//  This file was automatically generated and should not be edited.
//

import Foundation
import CoreData


extension RecordSet {

    @nonobjc public class func fetchRequest() -> NSFetchRequest<RecordSet> {
        return NSFetchRequest<RecordSet>(entityName: "RecordSet")
    }

    @NSManaged public var offset: Int64
    @NSManaged public var time: Date?
    @NSManaged public var dataContainer: DataContainer?
    @NSManaged public var group: RecordSetGroup?
    @NSManaged public var topic: KafkaTopic?
    @NSManaged public var uploadPart: UploadPart?

}
