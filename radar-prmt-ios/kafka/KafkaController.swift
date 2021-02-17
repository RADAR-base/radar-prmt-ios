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
        print("**KafkaController / init")
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
        print("**KafkaController / start1")
        print("**KafkaController / start2", self.isStarted)

        if !self.isStarted {
            print("**KafkaController / start3 isnotstarted")
            sender.start()
        }
        if let sendFlow = sendFlow {
            print("**KafkaController / start4 sendflowdispose")

            sendFlow.dispose()
        }

        if self.reachability == nil {
            print("**KafkaController / start5 reachablitity == nill")

            self.reachability = NetworkReachability(baseUrl: self.user.baseUrl)
        }
        self.isStarted = true
        print("**KafkaController / start6", self.isStarted)

        let queue = ConcurrentDispatchQueueScheduler(qos: .background)

        let timer: Observable<Void> = Observable<Int>.interval(config.interval, scheduler: queue) // 30 secs
            .do(onNext: {_ in print("**KafkaController / start7 timer")})
            .map { _ in () }

        let isConnected: Observable<Void> = self.reachability.subject.filter { !$0.isEmpty }
            .do(onNext: {_ in print("**KafkaController / start8 [isConnected]")})
            .map { [weak self] mode in
                print("**KafkaController / start9 updateConnection", mode)
                self?.updateConnection(to: mode)
                return ()
            }

        let isAuthorized = authController.validAuthentication()
            .do(onNext: {_ in print("**KafkaController / start10 / isAuthorized0")})
            .map { _ in () }
            .debounce(.seconds(15), scheduler: queue)
            .do(onNext: {_ in print("**KafkaController / start10 / isAuthorized1")})

        let uploadTrigger: Observable<Void> = Observable.merge(timer, isAuthorized, isConnected)
            .do(onNext: {_ in print("**KafkaController / start11 / uploadTrigger0")})
            .filter { [weak self] _ in self?.isConnectionValid ?? false }
            .share()
            .do(onNext: {_ in print("**KafkaController / start11 / uploadTrigger1")})

        let retryQueue: Observable<UploadQueueElement> = uploadTrigger
            .do(onNext: {_ in print("**KafkaController / start12 / retryQueue0")})
            .flatMapLatest { [weak self] _ in
                //print("fffff")
//                if let a = self?.reader.nextRetry(minimumPriority: self!.minimumPriority) {
//                    return a //self?.reader.nextRetry(minimumPriority: self!.minimumPriority)
//                }else{
//                    return Observable.empty()
//                }
                self?.reader.nextRetry(minimumPriority: self!.minimumPriority) ?? Observable.empty()
            }
            .do(onNext: {_ in print("**KafkaController / start12 / retryQueue1")})


        let uploadQueue: Observable<UploadQueueElement> = uploadTrigger
            .do(onNext: {_ in print("**KafkaController / start13 / uploadQueue0")})
            .flatMapLatest { [weak self] _ in self?.reader.nextInQueue(minimumPriority: self!.minimumPriority) ?? Observable.empty() }
            .do(onNext: {_ in print("**KafkaController / start13 / uploadQueue1")})

        let resendQueue: Observable<UploadQueueElement> = resendSubject.asObservable()
            .do(onNext: {_ in print("**KafkaController / start14 / resendQueue0")})
            .filterMap { v in
                print("**KafkaController / start14 / v", v)
                if let v = v {
                    print("**KafkaController / start14 / v -> if")
                    return .map(v)
                } else {
                    print("**KafkaController / start14 / v -> else")
                    return .ignore
                }
            }
            .do(onNext: {_ in print("**KafkaController / start14 / resendQueue1")})


//        Observable.merge(uploadQueue, retryQueue, resendQueue)
//            .do(onNext: {_ in print("==^^")})
//            .subscribeOn(queue)
//            .subscribe(onNext: { _ in
//                print("==**")
//            }).disposed(by: disposeBag)
        sendFlow = Observable.merge(uploadQueue, retryQueue, resendQueue)
            .do(onNext: {_ in print("**KafkaController / start15 / ")})
            .flatMap { [weak self] element in self?.prepareUpload(for: element) ?? Observable.empty() }
            .withLatestFrom(authController.validAuthentication()) { (handle, auth) in (handle, auth) }
            .subscribeOn(queue)
            .subscribe(onNext: { [weak self] (uploadHandle, auth) in
                print("==?")
                guard let self = self else { return }
                let (handle, hasMore) = uploadHandle
                print("**KafkaController / self.sender.send******")
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
        print("**KafkaController / isConnectionValid")
        print("**KafkaController / isConnectionValid", context.availableNetworkModes)
        
        if context.availableNetworkModes.isEmpty {
            print("**KafkaController / isConnectionValid isEmpty")
            reachability.listen()
            return false
        }
        
        if let retryServer = context.retryServer, retryServer.at > Date() {
            return false
        }
        return true
    }

    private func updateConnection(to mode: NetworkReachability.Mode) {
        print("**KafkaController / updateConnection1", mode)
        let newMode = self.context.didConnect(over: mode)
        print("**KafkaController / updateConnection2", newMode)
        
        //? why?
        if newMode == [.cellular, .wifiOrEthernet] {
            print("**KafkaController / updateConnection3")
            
            os_log("Network connection is available again. Restarting data uploads.")
            self.reachability.cancel()
            self.start()
        }
    }

    var minimumPriority: Int? {
        print("**KafkaController / minimumPriority")
        let availableModes = context.availableNetworkModes
        print("**KafkaController / minimumPriority", availableModes)

        if availableModes.contains(.wifiOrEthernet) {
            print("**KafkaController / minimumPriority / return nil")

            return nil
        } else {
            print("**KafkaController / minimumPriority -> listen")

            reachability.listen()
            return context.minimumPriorityForCellular
        }
    }

    func prepareUpload(for upload: UploadQueueElement) -> Observable<(UploadHandle, Bool)> {
        print("**KafkaController / prepareUpload")

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
