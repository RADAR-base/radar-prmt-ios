//
//  DataContainer+CoreDataProperties.swift
//  
//
//  Created by Joris Borgdorff on 11/04/2019.
//
//  This file was automatically generated and should not be edited.
//

import Foundation
import CoreData

extension DataContainer {

    @nonobjc public class func fetchRequest() -> NSFetchRequest<DataContainer> {
        return NSFetchRequest<DataContainer>(entityName: "DataContainer")
    }

    @NSManaged public var data: Data?
    @NSManaged public var recordData: RecordSet?

}
