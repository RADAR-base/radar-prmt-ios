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

class LocationManager : DataSourceManager, SourceManager {
    var locationTopic: AvroTopicCacheContext!
    let manager: CLLocationManager
    var name: String {
        get {
            return "location"
        }
    }

    fileprivate var locationReceiver: LocationReceiver!
    
    override init?(provider: DelegatedSourceProvider, topicWriter: AvroDataWriter, sourceId: String?) {
        manager = CLLocationManager()
        super.init(provider: provider, topicWriter: topicWriter, sourceId: sourceId)
        if let locTopic = define(topic: "ios_location", valueSchemaPath: "passive/phone/phone_relative_location") {
            locationTopic = locTopic
        } else {
            return nil
        }
        locationReceiver = LocationReceiver(manager: self, topic: locationTopic)
        manager.delegate = locationReceiver
    }

    override func start() {
        startReceivingSignificantLocationChanges()
        startReceivingLocalLocationChanges()
    }

    func startReceivingLocalLocationChanges() {
        switch CLLocationManager.authorizationStatus() {
        case .notDetermined:
            manager.requestWhenInUseAuthorization()
        case .authorizedAlways, .authorizedWhenInUse:
            break
        default:
            // User has not authorized access to location information.
            return
        }
        
        manager.startUpdatingLocation()
    }
    
    func startReceivingSignificantLocationChanges() {
        switch CLLocationManager.authorizationStatus() {
        case .notDetermined, .authorizedWhenInUse:
            manager.requestAlwaysAuthorization()
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
        
        manager.startMonitoringSignificantLocationChanges()
    }

    override func willCloseForeground() {
        manager.stopUpdatingLocation()
    }

    override func willClose() {
        manager.stopUpdatingLocation()
        manager.stopMonitoringSignificantLocationChanges()
    }
}

fileprivate class LocationReceiver : NSObject, CLLocationManagerDelegate {
    let manager: LocationManager
    let locationTopic: AvroTopicCacheContext
    
    init(manager: LocationManager, topic: AvroTopicCacheContext) {
        self.manager = manager
        self.locationTopic = topic
    }

    func locationManager(_ manager: CLLocationManager, didChangeAuthorization status: CLAuthorizationStatus) {
        switch status {
        case .authorizedAlways:
            self.manager.startReceivingSignificantLocationChanges()
            self.manager.startReceivingLocalLocationChanges()
        case .authorizedWhenInUse:
            self.manager.startReceivingLocalLocationChanges()
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
//
//    func locationManager(_ manager: CLLocationManager, didFailWithError error: Error) {
//        
//    }
}
