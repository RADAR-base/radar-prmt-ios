//
//  AuthController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 12/06/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Valet
import Foundation
import RxSwift
import os.log

class AuthController {
    let secureData = Valet.valet(with: Identifier(nonEmpty: "pRMT")!, accessibility: .afterFirstUnlock)
    let disposeBag = DisposeBag()
    var authorizer: MPAuthorizer?
    let auth: BehaviorSubject<Authorization?>
    let privacyPolicyAccepted: BehaviorSubject<Bool>
    let config: RadarConfiguration
    let userMetadata: BehaviorSubject<UserMetadata?>
    let controlQueue = SerialDispatchQueueScheduler(qos: .background)

    init(config: RadarConfiguration) {
        // Reset keychain on first load
        let defaults = UserDefaults.standard
        if !defaults.bool(forKey: "isInitialized") {
            secureData.removeAllObjects()
            defaults.set(true, forKey: "isInitialized")
        }
        self.config = config

        userMetadata = BehaviorSubject(value: UserMetadata(sourceTypes: [], sources: []))
        auth = BehaviorSubject(value: nil)

        let concurrentScheduler = ConcurrentDispatchQueueScheduler(qos: .background)

        privacyPolicyAccepted = BehaviorSubject<Bool>(value: false)
        privacyPolicyAccepted
            .distinctUntilChanged()
            .subscribeOn(concurrentScheduler)
            .subscribe(onNext: { UserDefaults.standard.set($0, forKey: "privacyPolicyAccepted") })
            .disposed(by: disposeBag)

        userMetadata
            .distinctUntilChanged()
            .subscribeOn(concurrentScheduler)
            .subscribe(onNext: { [weak self] metadata in
                if let metadata = metadata {
                    if !metadata.sourceTypes.isEmpty {
                        try? self?.secureData.store(codable: metadata, forKey: "userMetadata")
                    }
                } else {
                    self?.secureData.removeObject(forKey: "userMetadata")
                }
            })
            .disposed(by: disposeBag)

        self.config.config
            .filter { !$0.isEmpty }
            .distinctUntilChanged()
            .subscribeOn(controlQueue)
            .subscribe(onNext: { [weak self] config in
                guard let self = self else { return }
                if let clientId = config["oauth2_client_id"],
                    let clientSecret = config["oauth2_client_secret"] {
                    self.authorizer = MPAuthorizer(clientId: clientId, clientSecret: clientSecret)
                    if let auth = try? self.auth.value() as? MPOAuth {
                        auth.authorizer = self.authorizer
                    }
                }
            })
            .disposed(by: disposeBag)

        self.auth
            .subscribeOn(controlQueue)
            .subscribe(onNext: { [weak self] auth in
                guard let self = self else { return }
                if let auth = auth as? MPOAuth {
                    auth.isAuthorized
                        .subscribe(onNext: { [weak self] isAuth in
                            if auth.isUpdatedSinceCoded {
                                os_log("Storing updated authentication")
                                try? self?.secureData.store(codable: auth, forKey: "auth")
                            } else {
                                os_log("Authentication of user %@ unchanged, not storing", auth.userId)
                            }
                            if !isAuth {
                                auth.ensureValid()
                            }
                        })
                        .disposed(by: auth.disposeBag)
                } else {
                    self.privacyPolicyAccepted.onNext(false)
                }
            })
            .disposed(by: disposeBag)
    }

    func load() {
        controlQueue.schedule(Void()) { [weak self] _ in
            guard let self = self else { return Disposables.create() }
            self.privacyPolicyAccepted.onNext(UserDefaults.standard.bool(forKey: "privacyPolicyAccepted"))

            do {
                if let storedAuth = try self.secureData.load(type: MPOAuth.self, forKey: "auth") {
                    os_log("Loaded authentication")
                    storedAuth.authorizer = self.authorizer
                    self.auth.onNext(storedAuth)
                }
            } catch {
                os_log("Failed to decode stored authentication: %@", type: .error, error.localizedDescription)
            }
            return Disposables.create()
        }.disposed(by: disposeBag)
    }

    func reset() {
        secureData.removeObject(forKey: "auth")
        auth.onNext(nil)
        privacyPolicyAccepted.onNext(false)
        userMetadata.onNext(nil)
    }
}

struct UserMetadata: Equatable, Codable {
    let sourceTypes: [SourceType]
    let sources: [Source]
}

struct SourceType: Equatable, Codable {
    let id: String
    let producer: String
    let model: String
    let version: String
    let canRegisterDynamically: Bool
}

struct Source: Equatable, Codable {
    let type: SourceType
    let id: String?
    let name: String?
    let expectedName: String?
    let attributes: [String: String]?
}
