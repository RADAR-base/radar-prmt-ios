//
//  SourceController.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import RxSwift
import RxSwiftExt
import os.log

protocol ControlledQueue: class {
    var controlQueue: SchedulerType { get }
    var disposeBag: DisposeBag { get set }
}

class SourceController: ControlledQueue {
    let dataController: DataController
    var sources: [SourceManager]
    let sourceProviders: [DelegatedSourceProvider]
    let controlQueue: SchedulerType
    let finalDisposeBag = DisposeBag()
    var disposeBag = DisposeBag()
    var flusher: Disposable? = nil
    var hasFlusher: Bool = false

    init(config: BehaviorSubject<RadarState>, dataController: DataController) {
        self.dataController = dataController
        self.sources = []
        let providers: [SourceProvider] = [LocationProvider(), SpamProvider()]
        self.sourceProviders = providers.map { DelegatedSourceProvider($0) }
        self.controlQueue = SerialDispatchQueueScheduler(qos: .background)
        config
            .subscribeOn(controlQueue)
            .subscribe(onNext: { [weak self] state in
                guard let self = self else { return }
                self.load(state: state)
                if (!state.isReadyToSend || state.lifecycle == .terminated) {
                    self.close()
                } else if (state.lifecycle == .background) {
                    self.closeForeground()
                } else if (state.lifecycle == .active) {
                    self.start()
                }
            })
            .disposed(by: finalDisposeBag)
    }

    private func load(state: RadarState) {
        guard let plugins = state.config["plugins"] else { return }

        var newProviders = Set(plugins.split(separator: " ")
            .filter { !$0.isEmpty }
            .map { String($0) }
            .compactMap { pluginName in self.sourceProviders.first { $0.sourceDefinition.pluginName == pluginName }})

        if state.lifecycle == .background {
            newProviders = newProviders.filter { $0.sourceDefinition.supportsBackground }
        } else if state.lifecycle == .terminated || !state.isReadyToSend {
            newProviders = []
        }

        let existingProviders = Set(sources.map { $0.provider })
        let defunctProviders = existingProviders.subtracting(newProviders)
        let defunctIndex = sources.partition { defunctProviders.contains($0.provider)  }
        let defunctSources = sources[defunctIndex ..< sources.endIndex]
        sources.removeSubrange(defunctIndex ..< sources.endIndex)
        defunctSources.forEach { $0.close() }

        let newSources = newProviders.subtracting(existingProviders)
            .compactMap { $0.provide(writer: dataController.writer, authConfig: state) }

        newSources.forEach { $0.start() }
        sources += newSources
    }

    private func startFlusher() {
        Observable<Int>.interval(.seconds(10), scheduler: self.controlQueue)
            .subscribe(weak: self, onNext: { weakSelf in
                { _ in weakSelf.flush() }
            })
            .disposed(by: self.disposeBag)
    }

    func start() {
        schedule { [weak self] in
            guard let self = self else { return }
            if (!self.hasFlusher) {
                self.startFlusher()
                self.hasFlusher = true
            }
        }
    }

    func flush() {
        schedule { [weak self] in
            self?.sources.forEach { $0.flush() }
        }
    }

    func closeForeground() {
        schedule { [weak self] in
            self?.sources.forEach { $0.closeForeground() }
        }
    }

    func close() {
        schedule { [weak self] in
            // reset flusher and other disposables
            self?.disposeBag = DisposeBag()
        }
    }
}

extension ControlledQueue {
    func schedule(action: @escaping () -> Void) {
        controlQueue.schedule(Void()) { _ in
            action()
            return Disposables.create()
        }.disposed(by: disposeBag)
    }
}
