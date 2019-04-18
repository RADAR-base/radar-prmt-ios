//
//  File.swift
//  radar-prmt-iosTests
//
//  Created by Joris Borgdorff on 15/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import CoreData
@testable import radar_prmt_ios

class MockDataController {
    let reader: AvroDataExtractor
    let writer: AvroDataWriter

    let container: NSPersistentContainer

    init() {
        guard let objectModel = NSManagedObjectModel.mergedModel(from: [Bundle.main]) else {
            fatalError("Cannot instantiate data model")
        }
        container = NSPersistentContainer(name: "radar_prmt_ios", managedObjectModel: objectModel)
        let description = NSPersistentStoreDescription()
        description.type = NSInMemoryStoreType
        //        description.shouldAddStoreAsynchronously = false // Make it simpler in test env

        container.persistentStoreDescriptions = [description]
        container.loadPersistentStores { (description, error) in
            // Check if the data store is in memory
            precondition( description.type == NSInMemoryStoreType )

            // Check if creating container wrong
            if let error = error {
                fatalError("Create an in-mem coordinator failed \(error)")
            }
        }

        writer = AvroDataWriter(container: container)
        reader = AvroDataExtractor(container: container)
    }
}
