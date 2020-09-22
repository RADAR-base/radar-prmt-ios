//
//  KafkaController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import CoreData
import os.log
import RxSwift
import RxSwiftExt

class KafkaController {
    let sender: KafkaSender
    let context: KafkaSendContext
    let schemaRegistry: SchemaRegistryClient
    var config: KafkaControllerConfig {
        didSet {
            if tmpOldConfig != config && isStarted {
                start()
            }
        }
        willSet(value) {
            tmpOldConfig = config
        }
    }
    private var tmpOldConfig: KafkaControllerConfig? = KafkaControllerConfig(config: [:])

    var maxProcessing: Int = 10
    let authController: AuthController
    var reachability: NetworkReachability!
    var isStarted: Bool = false
    let reader: AvroDataExtractor
    let medium: RequestMedium = FileRequestMedium(writeDirectory: URL(fileURLWithPath: NSTemporaryDirectory()))
    let readContext: UploadContext
    let disposeBag = DisposeBag()
    var sendFlow: Disposable? = nil
    let user: User
    let resendSubject: BehaviorSubject<UploadQueueElement?> = BehaviorSubject(value: nil)

    init(config: [String: String], authController: AuthController, user: User, reader: AvroDataExtractor) {
        self.reader = reader
        self.user = user
        schemaRegistry = SchemaRegistryClient(baseUrl: user.baseUrl)
        self.authController = authController
        readContext = JsonUploadContext(user: user, medium: medium)
        context = DataKafkaSendContext(reader: reader, medium: medium)
        sender = KafkaSender(authController: authController, user: user, context: context)
        self.config = KafkaControllerConfig(config: config)
    }

    func start() {
        if !self.isStarted {
            sender.start()
        }
        if let sendFlow = sendFlow {
            sendFlow.dispose()
        }

        if self.reachability == nil {
            self.reachability = NetworkReachability(baseUrl: self.user.baseUrl)
        }
        self.isStarted = true

        let queue = ConcurrentDispatchQueueScheduler(qos: .background)

        let timer: Observable<Void> = Observable<Int>.interval(config.interval, scheduler: queue)
            .map { _ in () }

        let isConnected: Observable<Void> = self.reachability.subject.filter { !$0.isEmpty }
            .map { [weak self] mode in
                self?.updateConnection(to: mode)
                return ()
            }

        let isAuthorized = authController.validAuthentication()
            .map { _ in () }
            .debounce(.seconds(15), scheduler: queue)

        let uploadTrigger: Observable<Void> = Observable.merge(timer, isAuthorized, isConnected)
            .filter { [weak self] _ in self?.isConnectionValid ?? false }
            .share()

        let retryQueue: Observable<UploadQueueElement> = uploadTrigger
            .flatMapLatest { [weak self] _ in self?.reader.nextRetry(minimumPriority: self!.minimumPriority) ?? Observable.empty() }

        let uploadQueue: Observable<UploadQueueElement> = uploadTrigger
            .flatMapLatest { [weak self] _ in self?.reader.nextInQueue(minimumPriority: self!.minimumPriority) ?? Observable.empty() }

        let resendQueue: Observable<UploadQueueElement> = resendSubject.asObservable()
            .filterMap { v in
                if let v = v {
                    return .map(v)
                } else {
                    return .ignore
                }
            }

        sendFlow = Observable.merge(uploadQueue, retryQueue, resendQueue)
            .flatMap { [weak self] element in self?.prepareUpload(for: element) ?? Observable.empty() }
            .withLatestFrom(authController.validAuthentication()) { (handle, auth) in (handle, auth) }
            .subscribeOn(queue)
            .subscribe(onNext: { [weak self] (uploadHandle, auth) in
                guard let self = self else { return }
                let (handle, hasMore) = uploadHandle
                self.sender.send(handle: handle)
                if (hasMore) {
                    self.resendSubject.onNext(handle.uploadQueueElement)
                }
            }, onError: { error in
                os_log("Controller failed: %@", error.localizedDescription)
            })

        sendFlow?.disposed(by: disposeBag)
    }

    var isConnectionValid: Bool {
        if context.availableNetworkModes.isEmpty {
            reachability.listen()
            return false
        }
        if let retryServer = context.retryServer, retryServer.at > Date() {
            return false
        }
        return true
    }

    private func updateConnection(to mode: NetworkReachability.Mode) {
        let newMode = self.context.didConnect(over: mode)
        if newMode == [.cellular, .wifiOrEthernet] {
            os_log("Network connection is available again. Restarting data uploads.")
            self.reachability.cancel()
            self.start()
        }
    }

    var minimumPriority: Int? {
        let availableModes = context.availableNetworkModes
        if availableModes.contains(.wifiOrEthernet) {
            return nil
        } else {
            reachability.listen()
            return context.minimumPriorityForCellular
        }
    }

    func prepareUpload(for upload: UploadQueueElement) -> Observable<(UploadHandle, Bool)> {
        return schemaRegistry.requestSchemas(for: upload.topic)
            .filterMap { [weak self] result in
                switch result {
                case let .failure(err):
                    os_log("No schema found for topic %@: %@", upload.topic, err.localizedDescription)
                    self?.context.didFail(for: upload.topic, code: 0, message: "No schema found", recoverable: false)
                    return .ignore
                case let .success(pair):
                    return .map(pair)
                }
            }
            .flatMap { [weak self] (pair: (SchemaMetadata, SchemaMetadata)) -> Observable<(UploadHandle, Bool)> in
                guard let self = self else { return Observable<(UploadHandle, Bool)>.empty() }
                return self.reader.prepareUpload(for: upload, with: self.readContext, schemas: pair)
            }
    }
}

struct KafkaControllerConfig: Equatable {
    let interval: DispatchTimeInterval

    init(config: [String: String]) {
        interval = .seconds(Int(config["kafka_upload_interval", default: "30"]) ?? 30)
    }
}

enum ObservableError: Error {
    case unowned
    case illegalState
}
