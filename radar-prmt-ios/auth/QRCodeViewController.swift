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

class QRCodeViewController: UIViewController {
    let disposeBag = DisposeBag()

//    @IBOutlet weak var statusLabel: UILabel!
    @IBOutlet weak var qrPreview: QRPreviewView! {
        didSet {
            qrPreview.delegate = self
        }
    }
    
    var qrData: QRData? = nil {
        didSet {
            if qrData != nil {
                guard let url = URL(string: (qrData?.codeString)!) else {
                    return
                }
                appDelegate.authController.login(to: url)
                    .subscribeOn(MainScheduler.instance)
                    .subscribe(weak: self, onNext: { weakSelf in { mpauth in
                        os_log("Retrieved MetaToken")
                        DispatchQueue.main.async {
                            weakSelf.performSegue(withIdentifier: "mainFromQr", sender: self)
                        }
                    }}, onError: { weakSelf in { error in
                        os_log("Failed to retrieve MetaToken: %@", error.localizedDescription)
                        DispatchQueue.main.async {
                            if let mpError = error as? MPAuthError {
                                switch (mpError) {
                                case .unauthorized:
                                    weakSelf.showError(message: "This app is not correctly configured in the RADAR-base installation.")
                                    
                                case .tokenAlreadyUsed:
                                    weakSelf.showError(message: "This token has been used. Please generate a new one.")
                                    
                                default:
                                    weakSelf.showError(message: "Cannot log in: \(mpError.errorDescription!)")
                                }
                            } else {
                                weakSelf.showError(message: "Cannot log in.")
                            }
                        }
                    }})
                    .disposed(by: disposeBag)
            }
        }
    }
    
    private func showError(message: String, fields: [UITextField] = []) {
        fields.forEach {
            $0.backgroundColor = UIColor(hue: 0.0, saturation: 0.1, brightness: 1.0, alpha: 1.0)
        }
        os_log("Error registering token: %@", message)

        // create the alert
        let alert = UIAlertController(title: "Error registering token", message: message, preferredStyle: .alert)

        // add an action (button)
        alert.addAction(UIAlertAction(title: "OK", style: UIAlertAction.Style.default, handler: { [self] action in
            if !qrPreview.isRunning {
                qrPreview.startScanning()
            }
        }))

        // show the alert
        self.present(alert, animated: true, completion: nil)
    }
    
    override func viewDidLoad() {
        super.viewDidLoad()
    }
    
 
    override func viewWillAppear(_ animated: Bool) {
        super.viewWillAppear(animated)

        if !qrPreview.isRunning {
            qrPreview.startScanning()
        }
    }
    
    override func viewWillDisappear(_ animated: Bool) {
        super.viewWillDisappear(animated)
        if !qrPreview.isRunning {
            qrPreview.stopScanning()
        }
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
}


extension QRCodeViewController: QRPreviewViewDelegate {
    func qrScanningDidStop() {
    }
    
    func qrScanningDidFail() {
    }
    
    func qrScanningSucceededWithCode(_ str: String?) {
        self.qrData = QRData(codeString: str)
    }
}


extension QRCodeViewController {
    override func prepare(for segue: UIStoryboardSegue, sender: Any?) {
    }
}
