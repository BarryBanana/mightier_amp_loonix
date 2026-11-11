import 'dart:async';
import 'dart:typed_data';

import 'package:flutter_midi_command/flutter_midi_command.dart';

import 'BLEController.dart';

/// BLEController-compatible transport backed by the Linux ALSA USB MIDI API.
/// The existing device protocol code uses BLE-MIDI timestamp framing, so this
/// class removes that framing for USB output and restores it for USB input.
class UsbMidiBLEDevice extends BLEDevice {
  final MidiDevice midiDevice;

  UsbMidiBLEDevice(this.midiDevice);

  @override
  String get id => midiDevice.id;

  @override
  String get name => midiDevice.name;

  @override
  Stream<BleDeviceState> get state => const Stream<BleDeviceState>.empty();
}

class UsbMidiScanResult extends BLEScanResult {
  final UsbMidiBLEDevice usbDevice;

  UsbMidiScanResult(MidiDevice midiDevice)
      : usbDevice = UsbMidiBLEDevice(midiDevice) {
    device = usbDevice;
  }

  @override
  String get id => usbDevice.id;

  @override
  String get name => usbDevice.name;
}

class UsbMidiController extends BLEController {
  final MidiCommand _midi = MidiCommand();
  final StreamController<List<int>> _dataController =
      StreamController<List<int>>.broadcast();

  StreamSubscription<MidiPacket>? _rxSubscription;
  StreamSubscription<String>? _setupSubscription;

  UsbMidiBLEDevice? _selectedDevice;
  bool _connected = false;

  UsbMidiController(List<String> forcedDevices) : super(forcedDevices);

  @override
  BLEDevice? get connectedDevice => _connected ? _selectedDevice : null;

  @override
  Future<void> init(ScanResultsCallback callback) async {
    await super.init(callback);

    _rxSubscription = _midi.onMidiDataReceived?.listen((packet) {
      if (!_connected || _selectedDevice == null) return;
      if (packet.device.id != _selectedDevice!.id) return;

      // The app's existing parsers expect the two-byte BLE-MIDI timestamp
      // prefix before the MIDI status byte.
      _dataController.add([0x80, 0x80, ...packet.data]);
    });

    _setupSubscription = _midi.onMidiSetupChanged?.listen((event) {
      if (event == 'deviceConnected') {
        _connected = true;
        setMidiSetupStatus(MidiSetupStatus.deviceConnected);
      } else if (event == 'deviceDisconnected') {
        _connected = false;
        setMidiSetupStatus(MidiSetupStatus.deviceDisconnected);
      }
    });

    await _refreshDevices();
  }

  Future<void> _refreshDevices() async {
    final devices = await _midi.devices ?? <MidiDevice>[];
    final nuxDevices = devices
        .where((device) =>
            device.name.toUpperCase().contains('NUX') ||
            device.name.toUpperCase().contains('NMP-03'))
        .map((device) => UsbMidiScanResult(device))
        .toList();

    onScanResults(nuxDevices, <BLEScanResult>[]);
    if (nuxDevices.isNotEmpty) {
      setMidiSetupStatus(MidiSetupStatus.deviceFound);
    }
  }

  @override
  Future<bool> isAvailable() async => true;

  @override
  void startScanning() {
    _refreshDevices();
  }

  @override
  void stopScanning() {}

  @override
  Future<BLEConnection?> connectToDevice(BLEDevice device) async {
    if (device is! UsbMidiBLEDevice) return null;

    _selectedDevice = device;
    _midi.connectToDevice(device.midiDevice);
    return null;
  }

  @override
  void disconnectDevice() {
    final device = _selectedDevice;
    if (device != null) {
      _midi.disconnectDevice(device.midiDevice);
    }
  }

  @override
  StreamSubscription<List<int>> registerDataListener(
      Function(List<int>) listener) {
    return _dataController.stream.listen(listener);
  }

  @override
  bool get isWriteReady => _connected && _selectedDevice != null;

  @override
  Future<void> writeToCharacteristic(List<int> data, bool noResponse) async {
    if (!isWriteReady || data.length < 3) return;

    // Remove the BLE-MIDI two-byte timestamp header. BLE SysEx framing also
    // inserts a timestamp byte immediately before F7; USB MIDI does not.
    final raw = List<int>.from(data.sublist(2));
    if (raw.length >= 2 &&
        raw[raw.length - 2] == 0x80 &&
        raw.last == MidiMessageValues.sysExEnd) {
      raw.removeAt(raw.length - 2);
    }

    _midi.sendData(Uint8List.fromList(raw), deviceId: _selectedDevice!.id);
  }

  @override
  void dispose() {
    _rxSubscription?.cancel();
    _setupSubscription?.cancel();
    _dataController.close();
    _midi.teardown();
  }
}
