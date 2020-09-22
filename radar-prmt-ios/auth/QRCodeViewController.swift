//
//  File.swift
//  radar-prmt-ios
//
//  Created by Joris Borgdorff on 10/05/2019.
//  Copyright © 2019 Joris Borgdorff. All rights reserved.
//

import Foundation
import UIKit
import AVFoundation
import RxSwift
import os.log

class QRCodeViewController: UIViewController, AVCaptureMetadataOutputObjectsDelegate {
    @IBOutlet weak var qrPreview: QRPreviewView!

    let captureSession: AVCaptureSession
    let processingQueue: DispatchQueue
    let disposeBag = DisposeBag()

    override init(nibName nibNameOrNil: String?, bundle nibBundleOrNil: Bundle?) {
        captureSession = AVCaptureSession()
        processingQueue = DispatchQueue(label: "QR-processing", qos: .userInitiated)
        super.init(nibName: nibNameOrNil, bundle: nibBundleOrNil)
    }

    required init?(coder aDecoder: NSCoder) {
        captureSession = AVCaptureSession()
        processingQueue = DispatchQueue(label: "QR-processing", qos: .userInitiated)
        super.init(coder: aDecoder)
    }

    override func viewDidLoad() {
        super.viewDidLoad()
    }

    func setupCaptureSession() {
        guard let videoCaptureDevice = AVCaptureDevice.default(for: .video),
            let captureDeviceInput = try? AVCaptureDeviceInput(device: videoCaptureDevice),
            captureSession.canAddInput(captureDeviceInput) else {
                //The device is not available or can't be added to the session.
                return
        }

        let metadataOutput = AVCaptureMetadataOutput()
        guard captureSession.canAddOutput(metadataOutput) else {
            // The output can't be added to the session.
            return
        }

        captureSession.addInput(captureDeviceInput)
        captureSession.addOutput(metadataOutput)

        guard metadataOutput.availableMetadataObjectTypes.contains(.qr) else {
            // QR code metadata output is not available on this device
            return
        }

        metadataOutput.metadataObjectTypes = [.qr]
        metadataOutput.setMetadataObjectsDelegate(self, queue: processingQueue)

        qrPreview.videoPreviewLayer.session = captureSession

        captureSession.commitConfiguration()
        captureSession.startRunning()
    }

    static func requestAuthorization() -> Single<Bool> {
        return Single<Bool>.create { single in
            switch AVCaptureDevice.authorizationStatus(for: AVMediaType.video) {
            case .authorized:
                single(.success(true))
            // The user has previously granted permission to access the camera.
            case .notDetermined:
                // We have never requested access to the camera before.
                AVCaptureDevice.requestAccess(for: .video) { granted in
                    single(.success(granted))
                }
            case .denied, .restricted:
                // The user either previously denied the access request or the
                // camera is not available due to restrictions.
                single(.success(false))
            @unknown default:
                single(.success(false))
            }

            return Disposables.create()
        }
    }

    func metadataOutput(_ output: AVCaptureMetadataOutput, didOutput metadataObjects: [AVMetadataObject], from connection: AVCaptureConnection) {
        guard let readableObject = metadataObjects.first as? AVMetadataMachineReadableCodeObject,
                let stringValue = readableObject.stringValue,
                let url = URL(string: stringValue) else {
            return
        }
        captureSession.stopRunning()
        appDelegate.authController.login(to: url)
            .subscribeOn(MainScheduler.instance)
            .subscribe(weak: self, onNext: { weakSelf in { mpauth in
                os_log("Retrieved MetaToken")
                weakSelf.performSegue(withIdentifier: "mainFromQr", sender: self)
            }}, onError: { _ in { error in
                os_log("Failed to retrieve MetaToken: %@", error.localizedDescription)
            }})
            .disposed(by: disposeBag)
    }
}
