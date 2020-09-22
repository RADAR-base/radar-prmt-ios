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
    var authorizer: BehaviorSubject<MPClient?> = BehaviorSubject(value: nil)
    let auth = BehaviorSubject<OAuthToken?>(value: nil)
    let config: RadarConfiguration
    let user = BehaviorSubject<User?>(value: nil)
    let controlQueue = SerialDispatchQueueScheduler(qos: .background)
    let isLoaded = BehaviorSubject<Bool>(value: false)
    let triggerMetadataRefresh = PublishSubject<Bool>()

    init(config: RadarConfiguration) {
        // Reset keychain on first load
        let defaults = UserDefaults.standard
        if !defaults.bool(forKey: "isInitialized") {
            secureData.removeAllObjects()
            defaults.set(true, forKey: "isInitialized")
        }
        self.config = config

        self.config.config
            .filter { !$0.isEmpty }
            .distinctUntilChanged()
            .subscribeOn(self.controlQueue)
            .subscribe(onNext: { [weak self] config in
                guard let self = self else { return }
                let clientId = config["oauth2_client_id"] ?? "pRMT"
                if let clientSecret = config["oauth2_client_secret"] {
                    self.authorizer.onNext(MPClient(controller: self, clientId: clientId, clientSecret: clientSecret))
                } else {
                    os_log("OAuth 2.0 client secret %@ must be configured", config["oauth2_client_secret"] ?? "<empty>")
                }
            })
            .disposed(by: self.disposeBag)

        let concurrentScheduler = ConcurrentDispatchQueueScheduler(qos: .background)

        Observable<Bool>.merge([
            self.auth
                .compactMap { auth in
                    if let isValid = auth?.isValid, isValid {
                        return true
                    } else {
                        return nil
                    }
                },
            Observable<Int>.timer(.seconds(2), period: .seconds(3600), scheduler: concurrentScheduler)
                .map { _ in false },
            self.triggerMetadataRefresh
                .filter { !$0 }
                .throttle(.seconds(1800), scheduler: concurrentScheduler),
            self.triggerMetadataRefresh
                .filter { $0 }
            ])
            .debounce(.seconds(5), scheduler: concurrentScheduler)
            .withLatestFrom(Observable.combineLatest(self.authorizer, self.user, self.validAuthentication())) { (b, authValues) in authValues }
            .flatMapLatest { (authorizer, user, auth) -> Observable<User> in
                guard let authorizer = authorizer, let user = user else { return Observable<User>.empty() }
                return try authorizer.requestMetadata(for: user, authorizedBy: auth)
            }
            .retry()
            .subscribeOn(concurrentScheduler)
            .subscribe(onNext: { [weak self] user in
                self?.user.onNext(user)
            })
            .disposed(by: self.disposeBag)
    }

    func load() {
        controlQueue.schedule(Void()) { [weak self] _ in
            guard let self = self else { return Disposables.create() }
            do {
                if let storedAuth = try self.secureData.load(type: OAuthToken.self, forKey: "auth") {
                    os_log("Loaded authentication")
                    self.auth.onNext(storedAuth)
                }

                if let storedUser = try self.secureData.load(type: User.self, forKey: "user") {
                    os_log("Loaded user metadata")
                    self.user.onNext(storedUser)
                }
            } catch {
                os_log("Failed to decode stored authentication: %@", type: .error, error.localizedDescription)
            }
            self.isLoaded.onNext(true)

            self.user
                .skip(1)
                .distinctUntilChanged()
                .subscribeOn(self.controlQueue)
                .subscribe(onNext: { [weak self] user in
                    guard let self = self else { return }
                    if let user = user {
                        do {
                            try self.secureData.store(codable: user, forKey: "user")
                        } catch {
                            os_log("Failed to store user data", type: .error)
                        }
                    } else {
                        self.secureData.removeObject(forKey: "user")
                    }
                })
                .disposed(by: self.disposeBag)

            self.auth
                .skip(1)
                .distinctUntilChanged()
                .subscribeOn(self.controlQueue)
                .subscribe(onNext: { [weak self] auth in
                    guard let self = self else { return }
                    if let auth = auth {
                        os_log("Storing updated authentication")
                        do {
                            try self.secureData.store(codable: auth, forKey: "auth")
                        } catch {
                            os_log("Failed to store updated authentication: %@", error.localizedDescription)
                        }
                    } else {
                        self.secureData.removeObject(forKey: "auth")
                    }
                })
                .disposed(by: self.disposeBag)

            return Disposables.create()
        }.disposed(by: disposeBag)
    }

    func reset() {
        secureData.removeObject(forKey: "auth")
        auth.onNext(nil)
        user.onNext(nil)
    }

    func ensureRegistration(of source: Source) -> Observable<Source> {
        return Observable.combineLatest(authorizer, user, validAuthentication())
            .flatMap { (authorizer, user, auth) -> Observable<Source> in
                guard let authorizer = authorizer, let user = user else {
                    return Observable<Source>.empty()
                }
                return authorizer.ensureRegistration(of: source, for: user, authorizedBy: auth)
            }
            .retryWhen { [weak self] obsError in
                return obsError.flatMap { [weak self] (error: Error) -> Observable<Bool> in
                    if case MPAuthError.unauthorized = error, let self = self {
                        return self.validAuthentication()
                            .map { $0.isValid }
                    }
                    throw error
                }
            }
            .do(onNext: { [weak self] _ in self?.triggerMetadataRefresh.onNext(true) })
    }

    func login(to url: URL) -> Observable<(User, OAuthToken)> {
        return self.validAuthorizer
            .observeOn(self.controlQueue)
            .flatMap { authorizer in authorizer.metaTokenLogin(url: url) }
            .do(onNext: {[weak self] (user, token) in
                guard let self = self else { return }
                self.auth.onNext(token)
                self.user.onNext(user)

                let defaults = UserDefaults.standard
                defaults.set(user.baseUrl, forKey: "baseUrl")
            })
    }

    func validAuthentication() -> Observable<OAuthToken> {
        return Observable.combineLatest(self.validAuthorizer, self.user, self.auth)
            .flatMapLatest { (authorizer, user, auth) -> Observable<OAuthToken> in
                guard let auth = auth, let user = user, user.userId == auth.userId else {
                    throw MPAuthError.unauthorized
                }
                if auth.isValid {
                    return Observable.just(auth)
                } else {
                    return authorizer.refresh(for: user, auth: auth)
                }
            }
    }

    private var validAuthorizer: Observable<MPClient> {
        get {
            return self.authorizer
                .map { (authorizer) in
                    if let authorizer = authorizer {
                        return authorizer
                    } else {
                        throw MPAuthError.unreferenced
                    }
            }
        }
    }

    func requestMetadata() -> Observable<User> {
        return Observable.empty()
    }

    func acceptPrivacyPolicy(for userId: String) -> Observable<User> {
        return user
            .take(1)
            .compactMap { (user: User?) -> User? in
                if let user = user, user.userId == userId, !user.privacyPolicyAccepted {
                    return user
                } else {
                    return nil
                }
            }
            .map { user -> User in
                var user = user
                user.privacyPolicyAccepted = true
                return user
            }
            .do(onNext: { [weak self] (user: User) in
                self?.user.onNext(user)
            })
    }

    func invalidate(accessToken: String) {
        self.auth
            .take(1)
            .subscribeOn(self.controlQueue)
            .filter { oldAuth in
                if let oldAccessToken = oldAuth?.accessToken {
                    return "Bearer \(oldAccessToken)" == accessToken
                } else {
                    return false
                }
            }
            .subscribe(onNext: { oldAuth in
                self.auth.onNext(try? OAuthToken(refreshToken: oldAuth!.refreshToken))
            })
            .disposed(by: disposeBag)
    }
}
