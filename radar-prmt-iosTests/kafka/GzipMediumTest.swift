//
//  GzipMediumTest.swift
//  radar-prmt-iosTests
//
//  Created by Joris Borgdorff on 25/04/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import XCTest
@testable import radar_prmt_ios

class GzipMediumTest: XCTestCase {
    override func setUp() {
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }

    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
    }

    func testExample() throws {
        let medium = DataRequestMedium()
        let gzipMedium = GzipRequestMedium(medium: medium)

        let handle = try gzipMedium.start(upload: RecordSetUpload())
        let dataHandle = ((handle as! GzipMediumHandle).handle as! DataMediumHandle)
        XCTAssertEqual(Data([]), dataHandle.data)

        try handle.append(data: Data(base64Encoded: "aGVsbG8=")!) // = hello
        try handle.finalize()

        print(dataHandle.data.base64EncodedString())

        XCTAssertEqual("H4sIAAAAAAAAE8tIzcnJBwCGphA2BQAAAA==", dataHandle.data.base64EncodedString())
    }
}
