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
import RxSwift
import Valet
import RxAppState

@UIApplicationMain
class AppDelegate: UIResponder, UIApplicationDelegate {

    var window: UIWindow?

    lazy var dataController = { DataController() }()
    let latestConfig = BehaviorSubject<RadarState>(value: RadarState(
        lifecycle: .inactive, privacyPolicyAccepted: false, auth: nil, userMetadata: nil, config: [:]))
    var sourceController: SourceController!
    var kafkaController: KafkaController?
    private let disposeBag = DisposeBag()
    lazy var config: RadarConfiguration = { FirebaseRadarConfiguration() }()
    let controlQueue: SchedulerType = SerialDispatchQueueScheduler(qos: .background)
    var lastServerStatus = BehaviorSubject<KafkaEvent>(value: .disconnected(Date()))
    lazy var authController = { AuthController(config: config) }()

    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplication.LaunchOptionsKey: Any]?) -> Bool {
        guard NSClassFromString("XCTestCase") == nil else {
            // Do not run main application code in tests
            return true
        }

        authController.load()
        Observable.combineLatest(
                UIApplication.shared.rx.appState.distinctUntilChanged(),
                authController.privacyPolicyAccepted.distinctUntilChanged(),
                authController.auth,
                authController.userMetadata.distinctUntilChanged(),
                config.config.distinctUntilChanged())
            .map { RadarState(lifecycle: $0.0, privacyPolicyAccepted: $0.1, auth: $0.2, userMetadata: $0.3, config: $0.4) }
            .subscribeOn(controlQueue)
            .subscribe(onNext: { [weak self] state in
                self?.latestConfig.onNext(state)
            })
            .disposed(by: disposeBag)

        sourceController = SourceController(config: latestConfig, dataController: dataController)
        manageKafkaController()
        return true
    }

    func applicationWillResignActive(_ application: UIApplication) {
        // Sent when the application is about to move from active to inactive state. This can occur for certain types of temporary interruptions (such as an incoming phone call or SMS message) or when the user quits the application and it begins the transition to the background state.
        // Use this method to pause ongoing tasks, disable timers, and invalidate graphics rendering callbacks. Games should use this method to pause the game.
    }

    func applicationDidEnterBackground(_ application: UIApplication) {
        // Use this method to release shared resources, save user data, invalidate timers, and store enough application state information to restore your application to its current state in case it is terminated later.
        // If your application supports background execution, this method is called instead of applicationWillTerminate: when the user quits.
        self.dataController.saveContext()
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

    func manageKafkaController() {
        latestConfig
            .subscribeOn(controlQueue)
            .subscribe(onNext: { [weak self] state in
                guard let self = self else { return }

                if !state.isReadyToSend || state.lifecycle == .terminated {
                    self.stopKafkaController()
                } else if state.lifecycle == .background {
                    self.stopKafkaController()
                } else if state.lifecycle == .active, let auth = state.auth {
                    self.ensureKafkaController(auth: auth, config: state.config)
                }
        }).disposed(by: disposeBag)
    }

    func ensureKafkaController(auth: Authorization, config: [String: String]) {
        if let controller = kafkaController {
            os_log("Updating Kafka controller configuration")
            controller.config = KafkaControllerConfig(config: config)
        } else {
            os_log("Starting Kafka controller")
            let controller = KafkaController(config: config, auth: auth, reader: self.dataController.reader)
            controller.start()
            self.kafkaController = controller
            controller.context.lastEvent
                .subscribeOn(self.controlQueue)
                .subscribe(onNext: { [weak self] event in
                    self?.lastServerStatus.onNext(event)
                })
                .disposed(by: controller.disposeBag)
        }
    }

    func stopKafkaController() {
        os_log("Disposing Kafka controller")
        kafkaController = nil
        let value = try? lastServerStatus.value()
        if let value = value, case .disconnected(_) = value {
            // already disconnected
        } else {
            lastServerStatus.onNext(.disconnected(Date()))
        }
    }
}

struct RadarState {
    let lifecycle: AppState
    let privacyPolicyAccepted: Bool
    let auth: Authorization?
    let userMetadata: UserMetadata?
    let config: [String: String]

    var isReadyToSend: Bool {
        get {
            guard let auth = auth else { return false }

            return privacyPolicyAccepted && !config.isEmpty
                && (!auth.requiresUserMetadata || (userMetadata != nil && userMetadata?.sourceTypes.isEmpty == false))
        }
    }
}

extension UIViewController {
    var appDelegate: AppDelegate {
        return UIApplication.shared.delegate as! AppDelegate
    }
}
