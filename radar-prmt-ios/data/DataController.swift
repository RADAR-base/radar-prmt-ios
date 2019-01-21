//
//  File.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 12/12/2018.
//  Copyright © 2018 Joris Borgdorff. All rights reserved.
//

import Foundation
import CoreData

class DataController {
    let managedObjectContext: NSManagedObjectContext
    let queue: DispatchQueue

    init(completionClosure: @escaping () -> ()) {
        //This resource is the same name as your xcdatamodeld contained in your project
        guard let modelURL = Bundle.main.url(forResource: "radar_prmt_ios", withExtension:"momd") else {
            fatalError("Error loading model from bundle")
        }
        // The managed object model for the application. It is a fatal error for the application not to be able to find and load its model.
        guard let mom = NSManagedObjectModel(contentsOf: modelURL) else {
            fatalError("Error initializing mom from: \(modelURL)")
        }
        
        let psc = NSPersistentStoreCoordinator(managedObjectModel: mom)
        
        queue = DispatchQueue(label: "prmt_core_data", qos: .background, attributes: .concurrent)
        managedObjectContext = NSManagedObjectContext(concurrencyType: .privateQueueConcurrencyType)
        managedObjectContext.persistentStoreCoordinator = psc
        
        queue.async {
            guard let docURL = FileManager.default.urls(for: .documentDirectory, in: .userDomainMask).last else {
                fatalError("Unable to resolve document directory")
            }
            let storeURL = docURL.appendingPathComponent("radar_prmt_ios.sqlite")
            do {
                try psc.addPersistentStore(ofType: NSSQLiteStoreType, configurationName: nil, at: storeURL, options: nil)
                //The callback block is expected to complete the User Interface and therefore should be presented back on the main queue so that the user interface does not need to be concerned with which queue this call is coming from.
                DispatchQueue.main.sync(execute: completionClosure)
            } catch {
                fatalError("Error migrating store: \(error)")
            }
        }
    }
}
