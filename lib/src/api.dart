import 'dart:convert';

import 'package:aws_common/aws_common.dart';
import 'package:aws_signature_v4/aws_signature_v4.dart';
import 'package:cloudflare_r2/src/model/status.dart';
import 'package:intl/intl.dart';
import 'package:xml/xml.dart';

import 'model/object_info.dart';

/// S3-compatible storage client.
///
/// Supports Cloudflare R2, Wasabi, AWS S3, and other S3-compatible providers.
/// Each instance holds its own endpoint, region, and credentials, allowing
/// multiple concurrent connections to different regions/providers.
class S3Client {
  final String host;
  final String region;
  final AWSSigV4Signer _signer;
  final S3ServiceConfiguration _serviceConfiguration =
      S3ServiceConfiguration();

  /// Last HTTP status code from the most recent request on this instance.
  int? lastStatusCode;

  /// Create an S3Client for a specific endpoint.
  ///
  /// [host] - S3-compatible endpoint (e.g. 's3.eu-central-1.wasabisys.com'
  ///   or '<accountId>.r2.cloudflarestorage.com')
  /// [region] - signing region (e.g. 'eu-central-1', 'auto' for R2)
  /// [accessKeyId] - S3 access key
  /// [secretAccessKey] - S3 secret key
  S3Client({
    required this.host,
    required String accessKeyId,
    required String secretAccessKey,
    this.region = 'us-east-1',
  }) : _signer = AWSSigV4Signer(
          credentialsProvider: AWSCredentialsProvider(
            AWSCredentials(accessKeyId, secretAccessKey),
          ),
        );

  /// Create from Cloudflare R2 account ID. Targets EU jurisdiction
  /// endpoint (`.eu.r2.cloudflarestorage.com`).
  factory S3Client.r2({
    required String accountId,
    required String accessKeyId,
    required String secretAccessKey,
    String region = 'auto',
  }) {
    return S3Client(
      host: '$accountId.eu.r2.cloudflarestorage.com',
      accessKeyId: accessKeyId,
      secretAccessKey: secretAccessKey,
      region: region,
    );
  }

  /// Create from Wasabi region.
  factory S3Client.wasabi({
    required String region,
    required String accessKeyId,
    required String secretAccessKey,
  }) {
    return S3Client(
      host: 's3.$region.wasabisys.com',
      accessKeyId: accessKeyId,
      secretAccessKey: secretAccessKey,
      region: region,
    );
  }

  AWSCredentialScope get _credentialScope => AWSCredentialScope(
        region: region,
        service: AWSService.s3,
      );

  Future<List<int>> getObject({
    required String bucket,
    required String objectName,
    void Function(int received, int total)? onReceiveProgress,
  }) async {
    lastStatusCode = null;

    final urlRequest = AWSHttpRequest.get(
      Uri.https(host, '$bucket/$objectName'),
      headers: {AWSHeaders.host: host},
    );
    final signedUrl = await _signer.sign(
      urlRequest,
      credentialScope: _credentialScope,
      serviceConfiguration: _serviceConfiguration,
    );

    var expectedTotalBytes = 0;

    var send = signedUrl.send()
      ..responseProgress.listen((event) {
        if (expectedTotalBytes <= 0) return;
        onReceiveProgress?.call(event, expectedTotalBytes);
      }, onDone: () {
        expectedTotalBytes = -1;
      }, onError: (e) {
        expectedTotalBytes = -1;
        throw e;
      }, cancelOnError: true);

    var response = await send.response;
    expectedTotalBytes =
        int.tryParse(response.headers['content-length'] ?? '') ?? -1;
    lastStatusCode = response.statusCode;

    if (lastStatusCode != 200) {
      if (lastStatusCode == 404) throw Exception('File not found');
      if (lastStatusCode == 403) throw Exception('Access Denied');
      throw Exception('Failed to download file');
    }

    var data = await response.bodyBytes;
    onReceiveProgress?.call(expectedTotalBytes, expectedTotalBytes);
    expectedTotalBytes = -1;

    return data;
  }

  Future<int> getObjectSize({
    required String bucket,
    required String objectName,
  }) async {
    return (await getObjectInfo(bucket: bucket, objectName: objectName)).size;
  }

  Future<ObjectInfo> getObjectInfo({
    required String bucket,
    required String objectName,
  }) async {
    lastStatusCode = null;

    final urlRequest = AWSHttpRequest.head(
      Uri.https(host, '$bucket/$objectName'),
      headers: {
        AWSHeaders.host: host,
        'Accept-Encoding': 'identity',
      },
    );

    final signedRequest = await _signer.sign(
      urlRequest,
      credentialScope: _credentialScope,
      serviceConfiguration: _serviceConfiguration,
    );

    final response = await signedRequest.send().response;
    lastStatusCode = response.statusCode;
    if (lastStatusCode != 200) {
      if (lastStatusCode == 404) throw Exception('File not found');
      if (lastStatusCode == 403) throw Exception('Access Denied');
      throw Exception('Failed to get object info: $lastStatusCode');
    }

    final contentLength =
        int.tryParse(response.headers['content-length'] ?? '');
    final etag = response.headers['etag'];
    final lastModifiedHeader = response.headers['last-modified'];
    final DateFormat httpDateFormat =
        DateFormat('EEE, dd MMM yyyy HH:mm:ss \'GMT\'', 'en_US');
    final lastModified = lastModifiedHeader != null
        ? httpDateFormat.parseUtc(lastModifiedHeader)
        : null;

    if (contentLength == null) throw Exception('Content-Length header missing');
    if (etag == null) throw Exception('ETag header missing');
    if (lastModified == null) throw Exception('Last-Modified header missing');

    if (contentLength <= 0) {
      throw Exception('Invalid file size: $contentLength bytes');
    }

    return ObjectInfo(
      key: objectName,
      size: contentLength,
      lastModified: lastModified,
      eTag: etag,
    );
  }

  Future<Status> putObject({
    required String bucket,
    required String objectName,
    required List<int> objectBytes,
    String? contentType,
  }) async {
    lastStatusCode = null;

    final urlRequest = AWSHttpRequest.put(
      Uri.https(host, '$bucket/$objectName'),
      headers: {
        AWSHeaders.host: host,
        if (contentType != null) AWSHeaders.contentType: contentType,
        AWSHeaders.contentLength: objectBytes.length.toString(),
      },
      body: objectBytes,
    );
    final signedUrl = await _signer.sign(
      urlRequest,
      credentialScope: _credentialScope,
      serviceConfiguration: _serviceConfiguration,
    );

    final response = await signedUrl.send().response;
    lastStatusCode = response.statusCode;

    if (lastStatusCode != 200) {
      if (lastStatusCode == 403) throw Exception('Access Denied');
      if (lastStatusCode == 400) throw Exception('Bad Request');
      if (lastStatusCode == 409) throw Exception('Conflict');
      if (lastStatusCode == 415) throw Exception('Unsupported Media Type');
      throw Exception('Failed to upload file. Status Code: $lastStatusCode');
    }

    return Status(
      status: lastStatusCode,
      message: lastStatusCode == 200
          ? 'File uploaded successfully'
          : 'File created successfully',
    );
  }

  Future<Status> deleteObject({
    required String bucket,
    required String objectName,
  }) async {
    lastStatusCode = null;

    final urlRequest = AWSHttpRequest.delete(
      Uri.https(host, '$bucket/$objectName'),
      headers: {
        AWSHeaders.host: host,
        AWSHeaders.accept: '*/*',
      },
    );
    final signedUrl = await _signer.sign(
      urlRequest,
      credentialScope: _credentialScope,
      serviceConfiguration: _serviceConfiguration,
    );

    final response = await signedUrl.send().response;
    lastStatusCode = response.statusCode;

    if (![200, 202, 204].contains(lastStatusCode)) {
      if (lastStatusCode == 400) throw Exception('Bad Request');
      if (lastStatusCode == 403) throw Exception('Access Denied');
      throw Exception(
          'Failed to delete file. Status Code: $lastStatusCode');
    }

    String message = switch (lastStatusCode) {
      200 => 'File deleted successfully',
      204 => 'No content, file deleted successfully',
      202 => 'Request accepted, file will be deleted soon',
      _ => '',
    };
    return Status(status: lastStatusCode, message: message);
  }

  Future<Status> deleteObjects({
    required String bucket,
    required List<String> objectNames,
  }) async {
    lastStatusCode = null;

    final xmlBody =
        '<Delete>${objectNames.map((name) => '<Object><Key>$name</Key></Object>').join()}</Delete>';
    final urlRequest = AWSHttpRequest.post(
      Uri.https(host, bucket, {'delete': ''}),
      headers: {
        AWSHeaders.host: host,
        AWSHeaders.accept: '*/*',
        AWSHeaders.contentType: 'application/xml',
      },
      body: utf8.encode(xmlBody),
    );
    final signedUrl = await _signer.sign(
      urlRequest,
      credentialScope: _credentialScope,
      serviceConfiguration: _serviceConfiguration,
    );

    final response = await signedUrl.send().response;
    lastStatusCode = response.statusCode;

    if (![200, 202, 204].contains(lastStatusCode)) {
      if (lastStatusCode == 400) throw Exception('Bad Request');
      if (lastStatusCode == 403) throw Exception('Access Denied');
      throw Exception('Failed to delete files. Status Code: $lastStatusCode');
    }

    String message = switch (lastStatusCode) {
      200 => 'Files deleted successfully',
      204 => 'No content, file deleted successfully',
      202 => 'Request accepted, file will be deleted soon',
      _ => '',
    };
    return Status(status: lastStatusCode, message: message);
  }

  Future<String> getPresignedUrl({
    required String bucket,
    required String objectName,
    Duration expiresIn = const Duration(hours: 1),
    Map<String, String>? queryParameters,
    Map<String, String>? headers,
  }) async {
    final urlRequest = AWSHttpRequest.get(
      Uri.https(host, '$bucket/$objectName', queryParameters),
      headers: {
        AWSHeaders.host: host,
        ...?headers,
      },
    );

    final signedUrl = await _signer.presign(
      urlRequest,
      credentialScope: _credentialScope,
      serviceConfiguration: _serviceConfiguration,
      expiresIn: expiresIn,
    );

    return signedUrl.toString();
  }

  Future<String> putPresignedUrl({
    required String bucket,
    required String objectName,
    Duration expiresIn = const Duration(hours: 1),
    String? contentType,
    Map<String, String>? headers,
  }) async {
    if (expiresIn.inSeconds > 604800) {
      throw ArgumentError('expiresIn cannot exceed 7 days (604800 seconds)');
    }

    final urlRequest = AWSHttpRequest.put(
      Uri.https(host, '$bucket/$objectName'),
      headers: {
        AWSHeaders.host: host,
        if (contentType != null) AWSHeaders.contentType: contentType,
        ...?headers,
      },
    );

    final signedUrl = await _signer.presign(
      urlRequest,
      credentialScope: _credentialScope,
      serviceConfiguration: _serviceConfiguration,
      expiresIn: expiresIn,
    );

    return signedUrl.toString();
  }

  Future<List<ObjectInfo>> listObjectsV2({
    required String bucket,
    bool doPagination = true,
    int maxKeys = 1000,
    String? delimiter,
    String? prefix,
    String? encodingType,
    String? startAfter,
    String? continuationToken,
  }) async {
    lastStatusCode = null;

    final queryParams = {
      'list-type': '2',
      'max-keys': maxKeys.toString(),
      if (continuationToken != null) 'continuation-token': continuationToken,
      if (delimiter != null) 'delimiter': delimiter,
      if (prefix != null) 'prefix': prefix,
      if (encodingType != null) 'encoding-type': encodingType,
      if (startAfter != null) 'start-after': startAfter,
    };

    final urlRequest = AWSHttpRequest.get(
      Uri.https(host, bucket, queryParams),
      headers: {AWSHeaders.host: host},
    );
    final signedUrl = await _signer.sign(
      urlRequest,
      credentialScope: _credentialScope,
      serviceConfiguration: _serviceConfiguration,
    );

    final response = await signedUrl.send().response;
    lastStatusCode = response.statusCode;
    if (lastStatusCode != 200) {
      if (lastStatusCode == 404) throw Exception('Bucket not found');
      if (lastStatusCode == 403) throw Exception('Access Denied');
      throw Exception('Failed to list objects: $lastStatusCode');
    }

    final bodyBytes = await response.bodyBytes;
    final xml = String.fromCharCodes(bodyBytes);
    final document = XmlDocument.parse(xml);
    final Iterable<XmlElement> contents = document.findAllElements('Contents');
    final nextContinuationToken = document
        .findAllElements('NextContinuationToken')
        .singleOrNull
        ?.innerText;

    final List<ObjectInfo> objectNames = [];
    for (var node in contents) {
      var key = node.findElements('Key').singleOrNull?.innerText;
      var size = node.findElements('Size').singleOrNull?.innerText;
      var lastModified =
          node.findElements('LastModified').singleOrNull?.innerText;
      var eTag = node.findElements('ETag').singleOrNull?.innerText;
      var storageClass =
          node.findElements('StorageClass').singleOrNull?.innerText;

      if (key == null ||
          size == null ||
          lastModified == null ||
          eTag == null ||
          storageClass == null) {
        throw Exception('Failed to parse object info');
      }

      objectNames.add(ObjectInfo(
        key: key,
        size: int.parse(size),
        lastModified: DateTime.parse(lastModified),
        eTag: eTag,
        storageClass: storageClass,
      ));
    }
    if (doPagination == false) return objectNames;
    if (nextContinuationToken != null) {
      final nextObjectNames = await listObjectsV2(
        bucket: bucket,
        maxKeys: maxKeys,
        continuationToken: nextContinuationToken,
      );
      objectNames.addAll(nextObjectNames);
    }

    return objectNames;
  }
}

/// Legacy alias for backward compatibility.
@Deprecated('Use S3Client instead')
typedef CloudFlareR2 = S3Client;
