//
//  HKStepProtocol.swift
//  radar-prmt-ios
//
//  Created by Peyman Mohtashami on 02/03/2021.
//  Copyright © 2021 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
import os.log
import RxSwift
import HealthKit

class HKStepProtocol : SourceProtocol {
    weak var manager: SourceManager?
    let usesBackgroundHKStep: Bool
    let controlQueue = MainScheduler.instance
    var hkStepTopic: AvroTopicCacheContext!

    var healthStore: HKHealthStore?
    var statisticsCollectionQuery: HKStatisticsCollectionQuery?
    var sampleQuery: HKSampleQuery?
    var dataValues: Array<Any> = []
    
    init?(manager: SourceManager) {
        self.manager = manager
        self.usesBackgroundHKStep = manager.provider.pluginDefinition.supportsBackground
        
        if HKHealthStore.isHealthDataAvailable() {
            self.healthStore = HKHealthStore()
        }else{
            return
        }
    }

    func startScanning() -> Single<Source> {
        guard let manager = self.manager else { return Single.error(MPAuthError.unreferenced) }

        if let source = manager.findSource(where: { _ in true }) {
            os_log("Found matching source %@", source.id ?? "<unknown>")
            return manager.use(source: source, afterRegistration: false)
        } else {
            os_log("Did not find matching source. Registering a new one.")
            return manager.use(source: Source(type: manager.sourceType, id: nil, name: "hk_step", expectedName: nil, attributes: nil))
        }
    }

    func registerTopics() -> Bool {
        guard let hkStepTopic = self.manager?.define(topic: "ios_hk_step", valueSchemaPath: "passive/phone/phone_hk_step") else {
            return false
        }
        self.hkStepTopic = hkStepTopic
        return true
    }

    func startCollecting() {
        let stepType = HKQuantityType.quantityType(forIdentifier: HKQuantityTypeIdentifier.stepCount)!
        self.healthStore?.requestAuthorization(toShare: [stepType], read: [stepType]) {
            (succes, error) in
            if succes {
                // self.calculateDailyStepCountForPastWeek()
                self.calculateDailyStepCount()
            }else{
                print("Authorization is not successful")
            }
        }
    }

    func closeForeground() {
        if((self.sampleQuery) != nil){
            self.healthStore?.stop(self.sampleQuery!)
        }
    }

    func close() {
        if((self.sampleQuery) != nil){
            self.healthStore?.stop(self.sampleQuery!)
        }
    }

    func calculateDailyStepCount(){
        let stepType = HKSampleType.quantityType(forIdentifier: HKQuantityTypeIdentifier.stepCount)!
        let sort = [NSSortDescriptor(key: HKSampleSortIdentifierStartDate, ascending: false)]
       
        self.sampleQuery = HKSampleQuery(sampleType: stepType, predicate: nil, limit: 1, sortDescriptors: sort) {
            query, results, error in
                guard error == nil else { print("error"); return }
                self.printResultOfOneDay(results: results)
        }
        self.healthStore?.execute(sampleQuery!)
    }
    
    private func printResultOfOneDay(results:[HKSample]?) {
        guard let results = results else {
            return
        }
        for result in results {
            guard let result:HKQuantitySample = result as? HKQuantitySample else { return }
            os_log("Did update HK Step to step count: %f start date %f end date %f", type: .debug, result.quantity.doubleValue(for: HKUnit.count()), result.startDate.timeIntervalSince1970, result.endDate.timeIntervalSince1970)

            print("---------------------------------\n")
            print("Step count: \(result.quantity.doubleValue(for: HKUnit.count()))")
            print("quantityType: \(result.quantityType)")
            print("Start Date: \(result.startDate)")
            print("End Date: \(result.endDate)")
            print("Metadata: \(String(describing: result.metadata))")
            print("UUID: \(result.uuid)")
            print("Source: \(result.sourceRevision)")
            print("Device: \(String(describing: result.device))")
            print("---------------------------------\n")
            
            self.hkStepTopic.add(record: [
                "time": result.startDate.timeIntervalSince1970,
                "timeReceived": Date().timeIntervalSince1970,
                "offsetReference": 0,
                "provider": "UNKNOWN",
                "latitude": result.quantity.doubleValue(for: HKUnit.count()),
                "longitude": result.quantity.doubleValue(for: HKUnit.count()),
                "altitude": 0,
                "accuracy": 0,
                "speed": 0,
                "bearing": 0,
            ])
            
//            self.hkStepTopic.add(record: [
//                "timeReceived": Date().timeIntervalSince1970,
//                "startDate": result.startDate.timeIntervalSince1970,
//                "endDate": result.endDate.timeIntervalSince1970,
//                "provider": "UNKNOWN",
//                "stepCount": result.quantity.doubleValue(for: HKUnit.count()),
//                "metadata": String(describing: result.metadata),
//                "uuid": result.uuid,
//                "source": result.sourceRevision,
//                "device": String(describing: result.device)
//            ])
        }
    }
    
    func calculateDailyStepCountForPastWeek(){
        let stepType = HKSampleType.quantityType(forIdentifier: HKQuantityTypeIdentifier.stepCount)!
        
        let monday = createAnchorDate()
        let daily = DateComponents(day: 1)

        let exactlySevenDaysAgo = Calendar.current.date(byAdding: DateComponents(day: -7), to: Date())!
        let oneWeekAgo = HKQuery.predicateForSamples(withStart: exactlySevenDaysAgo, end: nil, options: .strictStartDate)

        self.statisticsCollectionQuery = HKStatisticsCollectionQuery(quantityType: stepType, quantitySamplePredicate: oneWeekAgo, options: .cumulativeSum, anchorDate: monday, intervalComponents: daily)

        self.statisticsCollectionQuery?.initialResultsHandler = {
            query, statisticsCollection, error in
            if let statisticsCollection = statisticsCollection {
                print("statisticsCollection Update UI")
                self.printResultsOfOneWeek(statisticsCollection)
            }else{
                print("No statisticsCollection")
            }
        }

        self.statisticsCollectionQuery?.statisticsUpdateHandler = { query, statistics, statisticsCollection, error in
            if let statisticsCollection = statisticsCollection {
                print("statisticsCollection Update UI - update")
                self.printResultsOfOneWeek(statisticsCollection)
            }else{
                print("No statisticsCollection - update")
            }
        }
        self.healthStore?.execute(statisticsCollectionQuery!)
    }
    
    // Return an anchor date for a statistics collection query.
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
    
    func printResultsOfOneWeek(_ statisticsCollection: HKStatisticsCollection){
        self.dataValues = []
        let startDate = Calendar.current.date(byAdding: .day, value: -6, to: Date())!
        let endDate = Date()

        statisticsCollection.enumerateStatistics(from: startDate, to: endDate){ [weak self]
            (statistics, stop) in

            if #available(iOS 13.0, *) {
                print(startDate, endDate, statistics.quantityType, statistics.sumQuantity() as Any, statistics.startDate, statistics.endDate, statistics.duration() as Any)
            } else {
                print(startDate, endDate, statistics.quantityType, statistics.sumQuantity() as Any, statistics.startDate, statistics.endDate)
            }
            self?.dataValues.append(statistics)
            
            // add to topic

        }
    }
}
