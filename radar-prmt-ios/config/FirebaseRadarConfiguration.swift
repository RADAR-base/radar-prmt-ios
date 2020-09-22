//
//  FirebaseRemoteConfig.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/05/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import Firebase
import RxSwift
import os.log

class FirebaseRadarConfiguration : RadarConfiguration {
    let localConfiguration = UserDefaults.standard
    let remoteConfig: RemoteConfig
    private var combinedNames = Set<String>()
    let config: BehaviorSubject<[String: String]>
    let disposeBag = DisposeBag()

    init() {
        self.config = BehaviorSubject(value: [:])
        FirebaseApp.configure()
        self.remoteConfig = RemoteConfig.remoteConfig()
        self.remoteConfig.setDefaults(fromPlist: "config")
        update()
        self.config
            .skip(1)
            .distinctUntilChanged()
            .subscribeOn(ConcurrentDispatchQueueScheduler(qos: .background))
            .subscribe(onNext: {
                os_log("RadarConfiguration: %@", "\($0 as AnyObject)")
            })
            .disposed(by: disposeBag)
    }

    func fetch(withDelay: TimeInterval = 0.0) {
        remoteConfig.fetch(withExpirationDuration: withDelay, completionHandler: Optional.some({[weak self] status, err in
            guard let self = self else { return }
            self.remoteConfig.activate(completion: { (_, _) in self.update() })
        }))
    }

    private func update() {
        let names = self.names
        var newConfig = [String: String](minimumCapacity: names.count)
        for name in names {
            newConfig[name] = self[name]
        }
        config.onNext(newConfig)
    }

    var names: Set<String> {
        return Set(localConfiguration.dictionaryRepresentation().keys)
            .union(remoteConfig.keys(withPrefix: nil))
    }

    subscript(name: String) -> String? {
        get {
            if let localValue = localConfiguration.string(forKey: name) {
                return localValue
            } else {
                return remoteConfig[name].stringValue
            }
        }
        set(value) {
            self.localConfiguration.setValue(value, forKey: name)
            update()
        }
    }

    subscript(name: String, defaultValue: String) -> String {
        if let localValue = localConfiguration.string(forKey: name) {
            return localValue
        } else if let remoteValue = remoteConfig[name].stringValue {
            return remoteValue
        } else {
            return defaultValue
        }
    }

    func reset(names: String...) {
        for name in names {
            localConfiguration.removeObject(forKey: name)
        }
    }
}
