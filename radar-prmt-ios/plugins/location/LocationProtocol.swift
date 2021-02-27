//
//  LocationManager.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 03/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import CoreLocation
import BlueSteel
import os.log
import RxSwift

class LocationProtocol : SourceProtocol {
    var locationManager: CLLocationManager!
    weak var manager: SourceManager?
    let disposeBag = DisposeBag()
    let usesBackgroundLocation: Bool
    let controlQueue = MainScheduler.instance

    fileprivate var locationReceiver: LocationReceiver!
    
    init?(manager: SourceManager) {
        self.manager = manager
        self.usesBackgroundLocation = manager.provider.pluginDefinition.supportsBackground
        post { weakSelf in
            weakSelf.locationManager = CLLocationManager()
        }
    }

    func startScanning() -> Single<Source> {
        guard let manager = self.manager else { return Single.error(MPAuthError.unreferenced) }

        if let source = manager.findSource(where: { _ in true }) {
            os_log("Found matching source %@", source.id ?? "<unknown>")
            return manager.use(source: source, afterRegistration: false)
        } else {
            os_log("Did not find matching source. Registering a new one.")
            return manager.use(source: Source(type: manager.sourceType, id: nil, name: "location", expectedName: nil, attributes: nil))
        }
    }

    func registerTopics() -> Bool {
        guard let locationTopic = self.manager?.define(topic: "ios_location", valueSchemaPath: "passive/phone/phone_relative_location") else {
            return false
        }
        locationReceiver = LocationReceiver(locationProtocol: self, topic: locationTopic)
        locationManager.delegate = locationReceiver
        return true
    }

    func startCollecting() {
        post { weakSelf in
            if weakSelf.usesBackgroundLocation {
                weakSelf.startReceivingSignificantLocationChanges()
            }
            weakSelf.startReceivingLocalLocationChanges()
        }
    }

    func startReceivingLocalLocationChanges() {
        switch CLLocationManager.authorizationStatus() {
        case .notDetermined:
            locationManager.requestWhenInUseAuthorization()
        case .authorizedAlways, .authorizedWhenInUse:
            break
        default:
            // User has not authorized access to location information.
            return
        }
        
        locationManager.startUpdatingLocation()
    }
    
    func startReceivingSignificantLocationChanges() {
        switch CLLocationManager.authorizationStatus() {
        case .notDetermined, .authorizedWhenInUse:
            locationManager.requestAlwaysAuthorization()
        case .authorizedAlways:
            break
        default:
            // User has not authorized access to location information.
            return
        }
        
        if !CLLocationManager.significantLocationChangeMonitoringAvailable() {
            // The service is not available.
            return
        }
        
        locationManager.startMonitoringSignificantLocationChanges()
    }

    func closeForeground() {
        post { weakSelf in
            weakSelf.locationManager.stopUpdatingLocation()
        }
    }

    func close() {
        post { weakSelf in
            if weakSelf.usesBackgroundLocation {
                weakSelf.locationManager.stopMonitoringSignificantLocationChanges()
            }
        }
    }

    private func post(action: @escaping (LocationProtocol) -> Void) {
        self.controlQueue
            .schedule(Void()) { [weak self] _ in
                if let self = self {
                    action(self)
                }
                return Disposables.create()
            }
            .disposed(by: self.disposeBag)
    }
}

fileprivate class LocationReceiver : NSObject, CLLocationManagerDelegate {
    let locationProtocol: LocationProtocol
    let locationTopic: AvroTopicCacheContext
    
    init(locationProtocol: LocationProtocol, topic: AvroTopicCacheContext) {
        self.locationProtocol = locationProtocol
        self.locationTopic = topic
    }

    func locationManager(_ manager: CLLocationManager, didChangeAuthorization status: CLAuthorizationStatus) {
        switch status {
        case .authorizedAlways:
            self.locationProtocol.startReceivingSignificantLocationChanges()
            self.locationProtocol.startReceivingLocalLocationChanges()
        case .authorizedWhenInUse:
            self.locationProtocol.startReceivingLocalLocationChanges()
        default:
            break
        }
    }
    
    func locationManager(_ manager: CLLocationManager,  didUpdateLocations locations: [CLLocation]) {
        for location in locations {
            os_log("Did update location to lat %f lon %f", type: .debug, location.coordinate.latitude, location.coordinate.longitude)
            locationTopic.add(record: [
                "time": location.timestamp.timeIntervalSince1970,
                "timeReceived": Date().timeIntervalSince1970,
                "offsetReference": 0,
                "provider": "UNKNOWN",
                "latitude": location.coordinate.latitude,
                "longitude": location.coordinate.longitude,
                "altitude": location.altitude,
                "accuracy": location.horizontalAccuracy,
                "speed": location.speed,
                "bearing": location.course,
                ])
        }
    }
}
