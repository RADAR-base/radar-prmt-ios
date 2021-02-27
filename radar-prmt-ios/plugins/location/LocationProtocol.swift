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

import HealthKit


class LocationProtocol : SourceProtocol {
    var locationManager: CLLocationManager!
    weak var manager: SourceManager?
    let disposeBag = DisposeBag()
    let usesBackgroundLocation: Bool
    let controlQueue = MainScheduler.instance

    fileprivate var locationReceiver: LocationReceiver!
    
    var healthStore: HKHealthStore?
    var query: HKStatisticsCollectionQuery?
    var dataValues: Array<Any> = []
    
    init?(manager: SourceManager) {
        print("**init1")
        self.manager = manager
        self.usesBackgroundLocation = manager.provider.pluginDefinition.supportsBackground
        post { weakSelf in
            print("**init2")

            //weakSelf.locationManager = CLLocationManager()
            let stepType = HKQuantityType.quantityType(forIdentifier: HKQuantityTypeIdentifier.stepCount)!
            print("**init3")
            
            if HKHealthStore.isHealthDataAvailable() {
                //presentHealthDataNotAvailableError()
                
                print("**Available")
                
                self.healthStore = HKHealthStore()
                self.healthStore?.requestAuthorization(toShare: [stepType], read: [stepType]) {
                    (succes, error) in
                    print("**healthStore?.requestAuthorization")
                    if succes {
                        print("**Successful")
                        self.calculateDailyStepCountForPastWeek()
                    }else{
                        print("**Not Successful")
                    }
                }
                
                
            }else{
                print("**Not Available")
                return
            }
            print("**init4")

        }
    }

    func startScanning() -> Single<Source> {
        print("**startScanning")
        guard let manager = self.manager else { return Single.error(MPAuthError.unreferenced) }

        if let source = manager.findSource(where: { _ in true }) {
            os_log("**Found matching source %@", source.id ?? "<unknown>")
            return manager.use(source: source, afterRegistration: false)
        } else {
            os_log("**Did not find matching source. Registering a new one.")
            return manager.use(source: Source(type: manager.sourceType, id: nil, name: "location", expectedName: nil, attributes: nil))
        }
    }

    func registerTopics() -> Bool {
        print("**registerTopics")
//        guard let locationTopic = self.manager?.define(topic: "ios_location", valueSchemaPath: "passive/phone/phone_relative_location") else {
//            return false
//        }
//        locationReceiver = LocationReceiver(locationProtocol: self, topic: locationTopic)
//        locationManager.delegate = locationReceiver
        return true
    }

    func startCollecting() {
        print("**startCollecting")
//        post { weakSelf in
//            if weakSelf.usesBackgroundLocation {
//                weakSelf.startReceivingSignificantLocationChanges()
//            }
//            weakSelf.startReceivingLocalLocationChanges()
//        }
    }

    func startReceivingLocalLocationChanges() {
        print("**startReceivingLocalLocationChanges")
//        switch CLLocationManager.authorizationStatus() {
//        case .notDetermined:
//            locationManager.requestWhenInUseAuthorization()
//        case .authorizedAlways, .authorizedWhenInUse:
//            break
//        default:
//            // User has not authorized access to location information.
//            return
//        }
//
//        locationManager.startUpdatingLocation()
    }
    
    func startReceivingSignificantLocationChanges() {
        print("**startReceivingSignificantLocationChanges")
//        switch CLLocationManager.authorizationStatus() {
//        case .notDetermined, .authorizedWhenInUse:
//            locationManager.requestAlwaysAuthorization()
//        case .authorizedAlways:
//            break
//        default:
//            // User has not authorized access to location information.
//            return
//        }
//
//        if !CLLocationManager.significantLocationChangeMonitoringAvailable() {
//            // The service is not available.
//            return
//        }
//
//        locationManager.startMonitoringSignificantLocationChanges()
    }

    func closeForeground() {
        print("**closeForeground")
//        post { weakSelf in
//            weakSelf.locationManager.stopUpdatingLocation()
//        }
    }

    func close() {
        print("**close")
//        post { weakSelf in
//            if weakSelf.usesBackgroundLocation {
//                weakSelf.locationManager.stopMonitoringSignificantLocationChanges()
//            }
//        }
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
    
    func calculateDailyStepCountForPastWeek(){
        print("calculateDailyStepCountForPastWeek")
        let stepType = HKSampleType.quantityType(forIdentifier: HKQuantityTypeIdentifier.stepCount)!
        let monday = createAnchorDate()
        let daily = DateComponents(day: 1)
        
        let exactlySevenDaysAgo = Calendar.current.date(byAdding: DateComponents(day: -7), to: Date())!
        let oneWeekAgo = HKQuery.predicateForSamples(withStart: exactlySevenDaysAgo, end: nil, options: .strictStartDate)
        
        print("stepType",stepType)
        print("monday",monday)
        print("daily",daily)
        print("exactlySevenDaysAgo",exactlySevenDaysAgo)
        print("oneWeekAgo",oneWeekAgo)
        
        self.query = HKStatisticsCollectionQuery(quantityType: stepType, quantitySamplePredicate: oneWeekAgo, options: .cumulativeSum, anchorDate: monday, intervalComponents: daily)
        
        self.query?.initialResultsHandler = {
            query, statisticsCollection, error in
            if let statisticsCollection = statisticsCollection {
                print("statisticsCollection Update UI")
                self.printResults(statisticsCollection)
            }else{
                print("No statisticsCollection")
            }
        }
        
        self.healthStore?.execute(query!)
    }
    
    /// Return an anchor date for a statistics collection query.
    func createAnchorDate() -> Date {
        // Set the arbitrary anchor date to Monday at 3:00 a.m.
        let calendar: Calendar = .current
        var anchorComponents = calendar.dateComponents([.day, .month, .year, .weekday], from: Date())
        let offset = (7 + (anchorComponents.weekday ?? 0) - 2) % 7
        
        anchorComponents.day! -= offset
        anchorComponents.hour = 3
        
        let anchorDate = calendar.date(from: anchorComponents)!
        
        return anchorDate
    }
    
    func printResults(_ statisticsCollection: HKStatisticsCollection){
        self.dataValues = []
        let startDate = Calendar.current.date(byAdding: .day, value: -6, to: Date())!
        let endDate = Date()
        
        statisticsCollection.enumerateStatistics(from: startDate, to: endDate){ [weak self]
            (statistics, stop) in
            print("===")
            print(statistics.endDate)
            if #available(iOS 13.0, *) {
                print(statistics.duration() as Any)
            } else {
                // Fallback on earlier versions
            }
            print(statistics.quantityType)
            print(statistics.sumQuantity() as Any)
            print("---")
            self?.dataValues.append(statistics)
        }
        
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
