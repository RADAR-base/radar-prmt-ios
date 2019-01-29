//
//  SchemaRegistryClient.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 23/01/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import BlueSteel
import os.log

class SchemaRegistryClient {
    static let CACHE_TIMEOUT = 3600 as TimeInterval

    let schemaUrl: URL
    private var cache: DictionaryCache<String, FetchedSchemaPair>
    private let queue: DispatchQueue

    init(baseUrl: URL) {
        var url = baseUrl
        url.appendPathComponent("schema", isDirectory: true)
        url.appendPathComponent("subjects", isDirectory: true)
        self.schemaUrl = baseUrl
        self.cache = DictionaryCache(invalidateAfter: SchemaRegistryClient.CACHE_TIMEOUT)
        self.queue = DispatchQueue(label: "Schema Registry", qos: .background)
    }

    func requestSchemas(for topic: AvroTopic, executing block: @escaping (FetchedSchemaPair?) -> ()) {
        let queue = self.queue
        queue.async { [weak self] in
            guard let self = self else { block(nil); return }
            let pair = self.cache[topic.name, default: FetchedSchemaPair()]

            if pair.isComplete {
                block(pair)
                return
            }

            if pair.keySchema == nil {
                self.makeRequest(for: topic, part: .key, executing: block)
            }

            if pair.valueSchema == nil {
                self.makeRequest(for: topic, part: .value, executing: block)
            }
        }
    }

    private func makeRequest(for topic: AvroTopic, part: RecordPart, executing block: @escaping (FetchedSchemaPair?) -> ()) {
        var url = schemaUrl
        url.appendPathComponent("\(topic.name)-\(part.rawValue)", isDirectory: true)
        url.appendPathComponent("versions", isDirectory: true)
        url.appendPathComponent("latest", isDirectory: false)
        var request = URLRequest(url: url)
        request.addValue("application/vnd.schemaregistry.v1+json", forHTTPHeaderField: "Accept")
        URLSession.shared.dataTask(with: request) { [weak self] (data, response, error) in
            guard let response = response as? HTTPURLResponse, response.statusCode == 200, let data = data else {
                os_log("Failed to retrieve schema at %@", type: .error, url.absoluteString)
                block(nil)
                return
            }
            guard let jsonObject = try? JSONSerialization.jsonObject(with: data) as! [String: Any],
                    let schemaId = jsonObject["id"] as? Int,
                    let schemaVersion = jsonObject["version"] as? Int,
                    let schemaString = jsonObject["schema"] as? String,
                    let schema = try? Schema(json: schemaString) else {
                os_log("Failed to parse schema at %@", type: .error, url.absoluteString)
                block(nil)
                return
            }

            let schemaMetadata = SchemaMetadata(id: schemaId, version: schemaVersion, schema: schema)
            self?.updatePair(part: part, for: topic, with: schemaMetadata, reportCompleted: block)
        }
    }

    private func updatePair(part: RecordPart, for topic: AvroTopic, with schemaMetadata: SchemaMetadata, reportCompleted block: @escaping (FetchedSchemaPair?) -> ()) {
        self.queue.async { [weak self] in
            guard let self = self else { return }
            var pair = self.cache[topic.name, default: FetchedSchemaPair()]
            switch part {
            case .key:
                pair.keySchema = schemaMetadata
            case .value:
                pair.valueSchema = schemaMetadata
            }
            self.cache[topic.name] = pair
            if pair.isComplete {
                block(pair)
            }
        }
    }

    private enum RecordPart: String {
        case key
        case value
    }
}

struct FetchedSchemaPair {
    var keySchema: SchemaMetadata? = nil
    var valueSchema: SchemaMetadata? = nil

    var isComplete: Bool {
        return keySchema != nil && valueSchema != nil
    }
}

struct SchemaMetadata {
    let id: Int
    let version: Int
    let schema: Schema
}
