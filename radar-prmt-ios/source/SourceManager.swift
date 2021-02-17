//
//  SourceManager.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 11/12/2018.
//  Copyright © 2018 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
import os.log
import RxSwift

class SourceManager {
    var status = BehaviorSubject<SourceStatus>(value: .initializing)

    var name: String {
        get {
            return "\(provider.pluginDefinition.pluginName)<\(activeSource?.id ?? "??")>"
        }
    }

    let topicWriter: AvroDataWriter

    var delegate: SourceProtocol!
    let encoder: AvroEncoder = GenericAvroEncoder(encoding: .binary)
    private let writeQueue: DispatchQueue = DispatchQueue(label: "prmt_core_data", qos: .background)
    private var dataCaches: [AvroTopicCacheContext] = []
    let provider: DelegatedSourceProvider
    let sourceType: SourceType
    var sources: [Source] = []
    var activeSource: Source? = nil
    let authControl: AuthController
    let state: RadarState
    let disposeBag = DisposeBag()

    init(provider: DelegatedSourceProvider, topicWriter: AvroDataWriter, sourceType: SourceType, authControl: AuthController, state: RadarState) {
        self.provider = provider
        self.topicWriter = topicWriter
        self.sourceType = sourceType
        self.authControl = authControl
        self.state = state
        
        self.sources = state.user?.sources ?? []
    }

    func define(topic name: String, valueSchemaPath: String, priority: Int16 = 0) -> AvroTopicCacheContext? {
        guard let sourceId = activeSource?.id else {
            os_log("Cannot define topic %@ without a source ID", type: .error, name)
            return nil
        }
        guard let path = Bundle.main.path(forResource: "radar-schemas.bundle/" + valueSchemaPath, ofType: "avsc") else {
            os_log("Schema in location radar-schemas.bundle/%@.avsc for topic %@ is not found", type: .error,
                   valueSchemaPath, name)
            return nil
        }
        guard let contents = try? String(contentsOfFile: path) else {
            os_log("Cannot read schema path %@ for topic %@", type: .error,
                   valueSchemaPath, name)
            return nil
        }

        do {
            let topic = try AvroTopic(name: name, valueSchema: contents)

            guard let dataGroup = topicWriter.register(topic: topic, sourceId: sourceId) else {
                os_log("Cannot create RecordDataGroup for topic %@", type: .error, name)
                return nil
            }

            let dataCache = AvroTopicCacheContext(topic: topic, dataGroup: dataGroup, queue: writeQueue, encoder: encoder, topicWriter: topicWriter)
            dataCaches.append(dataCache)
            return dataCache
        } catch {
            os_log("Cannot parse schema path %@ for topic %@: %@", type: .error, valueSchemaPath, name, error.localizedDescription)
            return nil
        }
    }

    func findSource(where predicate: (Source) -> Bool) -> Source? {
        print("!!! sources", self.sources)
        return self.sources.first(where: predicate)
    }

    func use(source: Source, afterRegistration: Bool = false) -> Single<Source> {
        self.activeSource = source
        if source.id == nil || afterRegistration {
            return self.authControl.ensureRegistration(of: source)
                .asSingle()
        } else {
            return Single.just(source)
        }
    }

    func start() {
        print("**!SourceManager / start")
        self.status.onNext(.scanning)
        print("**!SourceManager / start / delegate", delegate)

        delegate.startScanning()
            .subscribe(onSuccess: { [weak self] source in
                print("**!SourceManager / start / source", source)
                guard let self = self else { return }
                self.activeSource = source
                if self.delegate.registerTopics() {
                    self.status.onNext(.collecting)
                    self.delegate.startCollecting()
                } else {
                    os_log("**!Cannot register topics for %@", type: .error, self.name)
                    self.status.onNext(.invalid)
                }
            }, onError: { error in
                print("**!SourceManager / start / error", error)
                os_log("**!Failed to scan for source: %@", type: .error, error.localizedDescription)
                self.status.onNext(.disconnected)
            })
            .disposed(by: disposeBag)
    }

    func flush() {
        dataCaches.forEach { $0.flush() }
    }

    final func close() {
        self.status.onNext(.disconnecting)
        delegate.closeForeground()
        delegate.close()
        flush()
        self.status.onNext(.disconnected)
    }

    final func closeForeground() {
        self.status.onNext(.disconnecting)
        delegate.closeForeground()
        flush()
        self.status.onNext(.disconnected)
    }
}
