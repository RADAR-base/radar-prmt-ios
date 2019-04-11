//
//  UploadPart+CoreDataProperties.swift
//  
//
//  Created by Joris Borgdorff on 11/04/2019.
//
//  This file was automatically generated and should not be edited.
//

import Foundation
import CoreData


extension UploadPart {

    @nonobjc public class func fetchRequest() -> NSFetchRequest<UploadPart> {
        return NSFetchRequest<UploadPart>(entityName: "UploadPart")
    }

    @NSManaged public var upToOffset: Int64
    @NSManaged public var data: RecordSet?
    @NSManaged public var upload: RecordSetUpload?

}
