//
//  AppDelegate.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 10/12/2018.
//  Copyright © 2018 Joris Borgdorff. All rights reserved.
//

import UIKit
import CoreData
import os.log

@UIApplicationMain
class AppDelegate: UIResponder, UIApplicationDelegate {

    var window: UIWindow?
    var sourceController: SourceController!
    var kafkaController: KafkaController!
    var auth: Authorizer!

    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        // Override point for customization after application launch.
        guard NSClassFromString("XCTestCase") == nil else {
            // Do not run main application code in tests
            return true
        }
        sourceController = SourceController(dataController: dataController)
        sourceController.start()

        let baseUrl = URL(string: "https://radar-test.thehyve.net/")!
        auth = OAuth()
        kafkaController = KafkaController(baseURL: baseUrl, reader: dataController.reader, auth: auth)
        kafkaController.interval = 5
        kafkaController.start()

        repeatedFlush(using: DispatchQueue.global(qos: .userInitiated))

        return true
    }

    private func repeatedFlush(using queue: DispatchQueue) {
        queue.asyncAfter(deadline: .now() + 0.9) { [weak self] in
            os_log("Flush data", type: .debug)
            self?.sourceController.flush()
            self?.repeatedFlush(using: queue)
        }
    }

    func applicationWillResignActive(_ application: UIApplication) {
        // Sent when the application is about to move from active to inactive state. This can occur for certain types of temporary interruptions (such as an incoming phone call or SMS message) or when the user quits the application and it begins the transition to the background state.
        // Use this method to pause ongoing tasks, disable timers, and invalidate graphics rendering callbacks. Games should use this method to pause the game.
    }

    func applicationDidEnterBackground(_ application: UIApplication) {
        // Use this method to release shared resources, save user data, invalidate timers, and store enough application state information to restore your application to its current state in case it is terminated later.
        // If your application supports background execution, this method is called instead of applicationWillTerminate: when the user quits.
    }

    func applicationWillEnterForeground(_ application: UIApplication) {
        // Called as part of the transition from the background to the active state; here you can undo many of the changes made on entering the background.
    }

    func applicationDidBecomeActive(_ application: UIApplication) {
        // Restart any tasks that were paused (or not yet started) while the application was inactive. If the application was previously in the background, optionally refresh the user interface.
    }

    func applicationWillTerminate(_ application: UIApplication) {
        // Called when the application is about to terminate. Save data if appropriate. See also applicationDidEnterBackground:.
        // Saves changes in the application's managed object context before the application terminates.
        self.dataController.saveContext()
    }

    func application(_ application: UIApplication, handleEventsForBackgroundURLSession identifier: String, completionHandler: @escaping () -> Void) {
        // FIXME: handle background URL sessions
        completionHandler()
    }

    lazy var dataController = { DataController() }()
}

