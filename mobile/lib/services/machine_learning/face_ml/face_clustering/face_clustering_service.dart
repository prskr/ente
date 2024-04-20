import "dart:async";
import "dart:developer";
import "dart:isolate";
import "dart:math" show max;
import "dart:typed_data" show Uint8List;

import "package:computer/computer.dart";
import "package:flutter/foundation.dart" show kDebugMode;
import "package:logging/logging.dart";
import "package:ml_linalg/dtype.dart";
import "package:ml_linalg/vector.dart";
import "package:photos/generated/protos/ente/common/vector.pb.dart";
import 'package:photos/services/machine_learning/face_ml/face_clustering/cosine_distance.dart';
import "package:photos/services/machine_learning/face_ml/face_clustering/face_info_for_clustering.dart";
import "package:photos/services/machine_learning/face_ml/face_filtering/face_filtering_constants.dart";
import "package:photos/services/machine_learning/face_ml/face_ml_result.dart";
import "package:simple_cluster/simple_cluster.dart";
import "package:synchronized/synchronized.dart";

class FaceInfo {
  final String faceID;
  final double? faceScore;
  final double? blurValue;
  final bool? badFace;
  final List<double>? embedding;
  final Vector? vEmbedding;
  int? clusterId;
  String? closestFaceId;
  int? closestDist;
  int? fileCreationTime;
  FaceInfo({
    required this.faceID,
    this.faceScore,
    this.blurValue,
    this.badFace,
    this.embedding,
    this.vEmbedding,
    this.clusterId,
    this.fileCreationTime,
  });
}

enum ClusterOperation { linearIncrementalClustering, dbscanClustering }

class FaceClusteringService {
  final _logger = Logger("FaceLinearClustering");
  final _computer = Computer.shared();

  Timer? _inactivityTimer;
  final Duration _inactivityDuration = const Duration(minutes: 3);
  int _activeTasks = 0;

  final _initLock = Lock();

  late Isolate _isolate;
  late ReceivePort _receivePort = ReceivePort();
  late SendPort _mainSendPort;

  bool isSpawned = false;
  bool isRunning = false;

  static const kRecommendedDistanceThreshold = 0.24;
  static const kConservativeDistanceThreshold = 0.06;

  // singleton pattern
  FaceClusteringService._privateConstructor();

  /// Use this instance to access the FaceClustering service.
  /// e.g. `FaceLinearClustering.instance.predict(dataset)`
  static final instance = FaceClusteringService._privateConstructor();
  factory FaceClusteringService() => instance;

  Future<void> init() async {
    return _initLock.synchronized(() async {
      if (isSpawned) return;

      _receivePort = ReceivePort();

      try {
        _isolate = await Isolate.spawn(
          _isolateMain,
          _receivePort.sendPort,
        );
        _mainSendPort = await _receivePort.first as SendPort;
        isSpawned = true;

        _resetInactivityTimer();
      } catch (e) {
        _logger.severe('Could not spawn isolate', e);
        isSpawned = false;
      }
    });
  }

  Future<void> ensureSpawned() async {
    if (!isSpawned) {
      await init();
    }
  }

  /// The main execution function of the isolate.
  static void _isolateMain(SendPort mainSendPort) async {
    final receivePort = ReceivePort();
    mainSendPort.send(receivePort.sendPort);

    receivePort.listen((message) async {
      final functionIndex = message[0] as int;
      final function = ClusterOperation.values[functionIndex];
      final args = message[1] as Map<String, dynamic>;
      final sendPort = message[2] as SendPort;

      try {
        switch (function) {
          case ClusterOperation.linearIncrementalClustering:
            final result = FaceClusteringService.runLinearClustering(args);
            sendPort.send(result);
            break;
          case ClusterOperation.dbscanClustering:
            final result = FaceClusteringService._runDbscanClustering(args);
            sendPort.send(result);
            break;
        }
      } catch (e, stackTrace) {
        sendPort
            .send({'error': e.toString(), 'stackTrace': stackTrace.toString()});
      }
    });
  }

  /// The common method to run any operation in the isolate. It sends the [message] to [_isolateMain] and waits for the result.
  Future<dynamic> _runInIsolate(
    (ClusterOperation, Map<String, dynamic>) message,
  ) async {
    await ensureSpawned();
    _resetInactivityTimer();
    final completer = Completer<dynamic>();
    final answerPort = ReceivePort();

    _activeTasks++;
    _mainSendPort.send([message.$1.index, message.$2, answerPort.sendPort]);

    answerPort.listen((receivedMessage) {
      if (receivedMessage is Map && receivedMessage.containsKey('error')) {
        // Handle the error
        final errorMessage = receivedMessage['error'];
        final errorStackTrace = receivedMessage['stackTrace'];
        final exception = Exception(errorMessage);
        final stackTrace = StackTrace.fromString(errorStackTrace);
        _activeTasks--;
        completer.completeError(exception, stackTrace);
      } else {
        _activeTasks--;
        completer.complete(receivedMessage);
      }
    });

    return completer.future;
  }

  /// Resets a timer that kills the isolate after a certain amount of inactivity.
  ///
  /// Should be called after initialization (e.g. inside `init()`) and after every call to isolate (e.g. inside `_runInIsolate()`)
  void _resetInactivityTimer() {
    _inactivityTimer?.cancel();
    _inactivityTimer = Timer(_inactivityDuration, () {
      if (_activeTasks > 0) {
        _logger.info('Tasks are still running. Delaying isolate disposal.');
        // Optionally, reschedule the timer to check again later.
        _resetInactivityTimer();
      } else {
        _logger.info(
          'Clustering Isolate has been inactive for ${_inactivityDuration.inSeconds} seconds with no tasks running. Killing isolate.',
        );
        dispose();
      }
    });
  }

  /// Disposes the isolate worker.
  void dispose() {
    if (!isSpawned) return;

    isSpawned = false;
    _isolate.kill();
    _receivePort.close();
    _inactivityTimer?.cancel();
  }

  /// Runs the clustering algorithm [runLinearClustering] on the given [input], in an isolate.
  ///
  /// Returns the clustering result, which is a list of clusters, where each cluster is a list of indices of the dataset.
  ///
  /// WARNING: Make sure to always input data in the same ordering, otherwise the clustering can less less deterministic.
  Future<Map<String, int>?> predictLinear(
    Set<FaceInfoForClustering> input, {
    Map<int, int>? fileIDToCreationTime,
    double distanceThreshold = kRecommendedDistanceThreshold,
    double conservativeDistanceThreshold = kConservativeDistanceThreshold,
    bool useDynamicThreshold = true,
    int? offset,
  }) async {
    if (input.isEmpty) {
      _logger.warning(
        "Clustering dataset of embeddings is empty, returning empty list.",
      );
      return null;
    }
    if (isRunning) {
      _logger.warning("Clustering is already running, returning empty list.");
      return null;
    }

    isRunning = true;
    try {
      // Clustering inside the isolate
      _logger.info(
        "Start clustering on ${input.length} embeddings inside computer isolate",
      );
      final stopwatchClustering = Stopwatch()..start();
      // final Map<String, int> faceIdToCluster =
      //     await _runLinearClusteringInComputer(input);
      final Map<String, int> faceIdToCluster = await _runInIsolate(
        (
          ClusterOperation.linearIncrementalClustering,
          {
            'input': input,
            'fileIDToCreationTime': fileIDToCreationTime,
            'distanceThreshold': distanceThreshold,
            'conservativeDistanceThreshold': conservativeDistanceThreshold,
            'useDynamicThreshold': useDynamicThreshold,
            'offset': offset,
          }
        ),
      );
      // return _runLinearClusteringInComputer(input);
      _logger.info(
        'predictLinear Clustering executed in ${stopwatchClustering.elapsed.inSeconds} seconds',
      );

      isRunning = false;
      return faceIdToCluster;
    } catch (e, stackTrace) {
      _logger.severe('Error while running clustering', e, stackTrace);
      isRunning = false;
      rethrow;
    }
  }

  /// Runs the clustering algorithm [runLinearClustering] on the given [input], in computer, without any dynamic thresholding
  Future<Map<String, int>?> predictLinearComputer(
    Map<String, Uint8List> input, {
    Map<int, int>? fileIDToCreationTime,
    double distanceThreshold = kRecommendedDistanceThreshold,
  }) async {
    if (input.isEmpty) {
      _logger.warning(
        "Linear Clustering dataset of embeddings is empty, returning empty list.",
      );
      return {};
    }

    // Clustering inside the isolate
    _logger.info(
      "Start Linear clustering on ${input.length} embeddings inside computer isolate",
    );

    try {
      final clusteringInput = input
          .map((key, value) {
            return MapEntry(
              key,
              FaceInfoForClustering(
                faceID: key,
                embeddingBytes: value,
                faceScore: kMinimumQualityFaceScore + 0.01,
                blurValue: kLapacianDefault,
              ),
            );
          })
          .values
          .toSet();
      final startTime = DateTime.now();
      final faceIdToCluster = await _computer.compute(
        runLinearClustering,
        param: {
          "input": clusteringInput,
          "fileIDToCreationTime": fileIDToCreationTime,
          "distanceThreshold": distanceThreshold,
          "conservativeDistanceThreshold": distanceThreshold,
          "useDynamicThreshold": false,
        },
        taskName: "createImageEmbedding",
      ) as Map<String, int>;
      final endTime = DateTime.now();
      _logger.info(
        "Linear Clustering took: ${endTime.difference(startTime).inMilliseconds}ms",
      );
      return faceIdToCluster;
    } catch (e, s) {
      _logger.severe(e, s);
      rethrow;
    }
  }

  /// Runs the clustering algorithm [runCompleteClustering] on the given [input], in computer.
  ///
  /// WARNING: Only use on small datasets, as it is not optimized for large datasets.
  Future<Map<String, int>> predictCompleteComputer(
    Map<String, Uint8List> input, {
    Map<int, int>? fileIDToCreationTime,
    double distanceThreshold = kRecommendedDistanceThreshold,
    double mergeThreshold = 0.30,
  }) async {
    if (input.isEmpty) {
      _logger.warning(
        "Complete Clustering dataset of embeddings is empty, returning empty list.",
      );
      return {};
    }

    // Clustering inside the isolate
    _logger.info(
      "Start Complete clustering on ${input.length} embeddings inside computer isolate",
    );

    try {
      final startTime = DateTime.now();
      final faceIdToCluster = await _computer.compute(
        runCompleteClustering,
        param: {
          "input": input,
          "fileIDToCreationTime": fileIDToCreationTime,
          "distanceThreshold": distanceThreshold,
          "mergeThreshold": mergeThreshold,
        },
        taskName: "createImageEmbedding",
      ) as Map<String, int>;
      final endTime = DateTime.now();
      _logger.info(
        "Complete Clustering took: ${endTime.difference(startTime).inMilliseconds}ms",
      );
      return faceIdToCluster;
    } catch (e, s) {
      _logger.severe(e, s);
      rethrow;
    }
  }

  Future<Map<String, int>?> predictWithinClusterComputer(
    Map<String, Uint8List> input, {
    Map<int, int>? fileIDToCreationTime,
    double distanceThreshold = kRecommendedDistanceThreshold,
  }) async {
    _logger.info(
      '`predictWithinClusterComputer` called with ${input.length} faces and distance threshold $distanceThreshold',
    );
    try {
      if (input.length < 100) {
        final mergeThreshold = distanceThreshold + 0.06;
        _logger.info(
          'Running complete clustering on ${input.length} faces with distance threshold $mergeThreshold',
        );
        return predictCompleteComputer(
          input,
          fileIDToCreationTime: fileIDToCreationTime,
          mergeThreshold: mergeThreshold,
        );
      } else {
        _logger.info(
          'Running linear clustering on ${input.length} faces with distance threshold $distanceThreshold',
        );
        return predictLinearComputer(
          input,
          fileIDToCreationTime: fileIDToCreationTime,
          distanceThreshold: distanceThreshold,
        );
      }
    } catch (e, s) {
      _logger.severe(e, s);
      rethrow;
    }
  }

  Future<List<List<String>>> predictDbscan(
    Map<String, Uint8List> input, {
    Map<int, int>? fileIDToCreationTime,
    double eps = 0.3,
    int minPts = 5,
  }) async {
    if (input.isEmpty) {
      _logger.warning(
        "DBSCAN Clustering dataset of embeddings is empty, returning empty list.",
      );
      return [];
    }
    if (isRunning) {
      _logger.warning(
        "DBSCAN Clustering is already running, returning empty list.",
      );
      return [];
    }

    isRunning = true;

    // Clustering inside the isolate
    _logger.info(
      "Start DBSCAN clustering on ${input.length} embeddings inside computer isolate",
    );
    final stopwatchClustering = Stopwatch()..start();
    // final Map<String, int> faceIdToCluster =
    //     await _runLinearClusteringInComputer(input);
    final List<List<String>> clusterFaceIDs = await _runInIsolate(
      (
        ClusterOperation.dbscanClustering,
        {
          'input': input,
          'fileIDToCreationTime': fileIDToCreationTime,
          'eps': eps,
          'minPts': minPts,
        }
      ),
    );
    // return _runLinearClusteringInComputer(input);
    _logger.info(
      'DBSCAN Clustering executed in ${stopwatchClustering.elapsed.inSeconds} seconds',
    );

    isRunning = false;

    return clusterFaceIDs;
  }

  static Map<String, int> runLinearClustering(Map args) {
    // final input = args['input'] as Map<String, (int?, Uint8List)>;
    final input = args['input'] as Set<FaceInfoForClustering>;
    final fileIDToCreationTime = args['fileIDToCreationTime'] as Map<int, int>?;
    final distanceThreshold = args['distanceThreshold'] as double;
    final conservativeDistanceThreshold =
        args['conservativeDistanceThreshold'] as double;
    final useDynamicThreshold = args['useDynamicThreshold'] as bool;
    final offset = args['offset'] as int?;

    log(
      "[ClusterIsolate] ${DateTime.now()} Copied to isolate ${input.length} faces",
    );

    // Organize everything into a list of FaceInfo objects
    final List<FaceInfo> faceInfos = [];
    for (final face in input) {
      faceInfos.add(
        FaceInfo(
          faceID: face.faceID,
          faceScore: face.faceScore,
          blurValue: face.blurValue,
          badFace: face.faceScore < kMinimumQualityFaceScore ||
              face.blurValue < kLaplacianSoftThreshold ||
              (face.blurValue < kLaplacianVerySoftThreshold &&
                  face.faceScore < kMediumQualityFaceScore) ||
              face.isSideways,
          vEmbedding: Vector.fromList(
            EVector.fromBuffer(face.embeddingBytes).values,
            dtype: DType.float32,
          ),
          clusterId: face.clusterId,
          fileCreationTime:
              fileIDToCreationTime?[getFileIdFromFaceId(face.faceID)],
        ),
      );
    }

    // Sort the faceInfos based on fileCreationTime, in ascending order, so oldest faces are first
    if (fileIDToCreationTime != null) {
      faceInfos.sort((a, b) {
        if (a.fileCreationTime == null && b.fileCreationTime == null) {
          return 0;
        } else if (a.fileCreationTime == null) {
          return 1;
        } else if (b.fileCreationTime == null) {
          return -1;
        } else {
          return a.fileCreationTime!.compareTo(b.fileCreationTime!);
        }
      });
    }

    // Sort the faceInfos such that the ones with null clusterId are at the end
    final List<FaceInfo> facesWithClusterID = <FaceInfo>[];
    final List<FaceInfo> facesWithoutClusterID = <FaceInfo>[];
    for (final FaceInfo faceInfo in faceInfos) {
      if (faceInfo.clusterId == null) {
        facesWithoutClusterID.add(faceInfo);
      } else {
        facesWithClusterID.add(faceInfo);
      }
    }
    final alreadyClusteredCount = facesWithClusterID.length;
    final sortedFaceInfos = <FaceInfo>[];
    sortedFaceInfos.addAll(facesWithClusterID);
    sortedFaceInfos.addAll(facesWithoutClusterID);

    log(
      "[ClusterIsolate] ${DateTime.now()} Clustering ${facesWithoutClusterID.length} new faces without clusterId, and $alreadyClusteredCount faces with clusterId",
    );

    // Make sure the first face has a clusterId
    final int totalFaces = sortedFaceInfos.length;
    int dynamicThresholdCount = 0;

    if (sortedFaceInfos.isEmpty) {
      return {};
    }

    // Start actual clustering
    log(
      "[ClusterIsolate] ${DateTime.now()} Processing $totalFaces faces in total in this round ${offset != null ? "on top of ${offset + facesWithClusterID.length} earlier processed faces" : ""}",
    );
    // set current epoch time as clusterID
    int clusterID = DateTime.now().microsecondsSinceEpoch;
    if (facesWithClusterID.isEmpty) {
      // assign a clusterID to the first face
      sortedFaceInfos[0].clusterId = clusterID;
      clusterID++;
    }
    final stopwatchClustering = Stopwatch()..start();
    for (int i = 1; i < totalFaces; i++) {
      // Incremental clustering, so we can skip faces that already have a clusterId
      if (sortedFaceInfos[i].clusterId != null) {
        clusterID = max(clusterID, sortedFaceInfos[i].clusterId!);
        continue;
      }

      int closestIdx = -1;
      double closestDistance = double.infinity;
      late double thresholdValue;
      if (useDynamicThreshold) {
        thresholdValue = sortedFaceInfos[i].badFace!
            ? conservativeDistanceThreshold
            : distanceThreshold;
        if (sortedFaceInfos[i].badFace!) dynamicThresholdCount++;
      } else {
        thresholdValue = distanceThreshold;
      }
      if (i % 250 == 0) {
        log("[ClusterIsolate] ${DateTime.now()} Processed ${offset != null ? i + offset : i} faces");
      }
      for (int j = i - 1; j >= 0; j--) {
        late double distance;
        if (sortedFaceInfos[i].vEmbedding != null) {
          distance = 1.0 -
              sortedFaceInfos[i]
                  .vEmbedding!
                  .dot(sortedFaceInfos[j].vEmbedding!);
        } else {
          distance = cosineDistForNormVectors(
            sortedFaceInfos[i].embedding!,
            sortedFaceInfos[j].embedding!,
          );
        }
        if (distance < closestDistance) {
          if (sortedFaceInfos[j].badFace! &&
              distance > conservativeDistanceThreshold) {
            continue;
          }
          closestDistance = distance;
          closestIdx = j;
        }
      }

      if (closestDistance < thresholdValue) {
        if (sortedFaceInfos[closestIdx].clusterId == null) {
          // Ideally this should never happen, but just in case log it
          log(
            " [ClusterIsolate] [WARNING] ${DateTime.now()} Found new cluster $clusterID",
          );
          clusterID++;
          sortedFaceInfos[closestIdx].clusterId = clusterID;
        }
        sortedFaceInfos[i].clusterId = sortedFaceInfos[closestIdx].clusterId;
      } else {
        clusterID++;
        sortedFaceInfos[i].clusterId = clusterID;
      }
    }

    // Finally, assign the new clusterId to the faces
    final Map<String, int> newFaceIdToCluster = {};
    for (final faceInfo in sortedFaceInfos.sublist(alreadyClusteredCount)) {
      newFaceIdToCluster[faceInfo.faceID] = faceInfo.clusterId!;
    }

    stopwatchClustering.stop();
    log(
      ' [ClusterIsolate] ${DateTime.now()} Clustering for ${sortedFaceInfos.length} embeddings executed in ${stopwatchClustering.elapsedMilliseconds}ms',
    );
    if (useDynamicThreshold) {
      log(
        "[ClusterIsolate] ${DateTime.now()} Dynamic thresholding: $dynamicThresholdCount faces had a low face score or low blur clarity",
      );
    }

    // analyze the results
    FaceClusteringService._analyzeClusterResults(sortedFaceInfos);

    return newFaceIdToCluster;
  }

  static void _analyzeClusterResults(List<FaceInfo> sortedFaceInfos) {
    if (!kDebugMode) return;
    final stopwatch = Stopwatch()..start();

    final Map<String, int> faceIdToCluster = {};
    for (final faceInfo in sortedFaceInfos) {
      faceIdToCluster[faceInfo.faceID] = faceInfo.clusterId!;
    }

    //  Find faceIDs that are part of a cluster which is larger than 5 and are new faceIDs
    final Map<int, int> clusterIdToSize = {};
    faceIdToCluster.forEach((key, value) {
      if (clusterIdToSize.containsKey(value)) {
        clusterIdToSize[value] = clusterIdToSize[value]! + 1;
      } else {
        clusterIdToSize[value] = 1;
      }
    });

    // print top 10 cluster ids and their sizes based on the internal cluster id
    final clusterIds = faceIdToCluster.values.toSet();
    final clusterSizes = clusterIds.map((clusterId) {
      return faceIdToCluster.values.where((id) => id == clusterId).length;
    }).toList();
    clusterSizes.sort();
    // find clusters whose size is greater than 1
    int oneClusterCount = 0;
    int moreThan5Count = 0;
    int moreThan10Count = 0;
    int moreThan20Count = 0;
    int moreThan50Count = 0;
    int moreThan100Count = 0;

    for (int i = 0; i < clusterSizes.length; i++) {
      if (clusterSizes[i] > 100) {
        moreThan100Count++;
      } else if (clusterSizes[i] > 50) {
        moreThan50Count++;
      } else if (clusterSizes[i] > 20) {
        moreThan20Count++;
      } else if (clusterSizes[i] > 10) {
        moreThan10Count++;
      } else if (clusterSizes[i] > 5) {
        moreThan5Count++;
      } else if (clusterSizes[i] == 1) {
        oneClusterCount++;
      }
    }

    // print the metrics
    log(
      "[ClusterIsolate]  Total clusters ${clusterIds.length}: \n oneClusterCount $oneClusterCount \n moreThan5Count $moreThan5Count \n moreThan10Count $moreThan10Count \n moreThan20Count $moreThan20Count \n moreThan50Count $moreThan50Count \n moreThan100Count $moreThan100Count",
    );
    stopwatch.stop();
    log(
      "[ClusterIsolate]  Clustering additional analysis took ${stopwatch.elapsedMilliseconds} ms",
    );
  }

  static Map<String, int> runCompleteClustering(Map args) {
    final input = args['input'] as Map<String, Uint8List>;
    final fileIDToCreationTime = args['fileIDToCreationTime'] as Map<int, int>?;
    final distanceThreshold = args['distanceThreshold'] as double;
    final mergeThreshold = args['mergeThreshold'] as double;

    log(
      "[CompleteClustering] ${DateTime.now()} Copied to isolate ${input.length} faces for clustering",
    );

    // Organize everything into a list of FaceInfo objects
    final List<FaceInfo> faceInfos = [];
    for (final entry in input.entries) {
      faceInfos.add(
        FaceInfo(
          faceID: entry.key,
          vEmbedding: Vector.fromList(
            EVector.fromBuffer(entry.value).values,
            dtype: DType.float32,
          ),
          fileCreationTime:
              fileIDToCreationTime?[getFileIdFromFaceId(entry.key)],
        ),
      );
    }

    // Sort the faceInfos based on fileCreationTime, in ascending order, so oldest faces are first
    if (fileIDToCreationTime != null) {
      faceInfos.sort((a, b) {
        if (a.fileCreationTime == null && b.fileCreationTime == null) {
          return 0;
        } else if (a.fileCreationTime == null) {
          return 1;
        } else if (b.fileCreationTime == null) {
          return -1;
        } else {
          return a.fileCreationTime!.compareTo(b.fileCreationTime!);
        }
      });
    }

    if (faceInfos.isEmpty) {
      return {};
    }
    final int totalFaces = faceInfos.length;

    // Start actual clustering
    log(
      "[CompleteClustering] ${DateTime.now()} Processing $totalFaces faces in one single round of complete clustering",
    );

    // set current epoch time as clusterID
    int clusterID = DateTime.now().microsecondsSinceEpoch;

    // Start actual clustering
    final Map<String, int> newFaceIdToCluster = {};
    final stopwatchClustering = Stopwatch()..start();
    for (int i = 0; i < totalFaces; i++) {
      if ((i + 1) % 250 == 0) {
        log("[CompleteClustering] ${DateTime.now()} Processed ${i + 1} faces");
      }
      if (faceInfos[i].clusterId != null) continue;
      int closestIdx = -1;
      double closestDistance = double.infinity;
      for (int j = 0; j < totalFaces; j++) {
        if (i == j) continue;
        final double distance =
            1.0 - faceInfos[i].vEmbedding!.dot(faceInfos[j].vEmbedding!);
        if (distance < closestDistance) {
          closestDistance = distance;
          closestIdx = j;
        }
      }

      if (closestDistance < distanceThreshold) {
        if (faceInfos[closestIdx].clusterId == null) {
          clusterID++;
          faceInfos[closestIdx].clusterId = clusterID;
        }
        faceInfos[i].clusterId = faceInfos[closestIdx].clusterId!;
      } else {
        clusterID++;
        faceInfos[i].clusterId = clusterID;
      }
    }

    // Now calculate the mean of the embeddings for each cluster
    final Map<int, List<FaceInfo>> clusterIdToFaceInfos = {};
    for (final faceInfo in faceInfos) {
      if (clusterIdToFaceInfos.containsKey(faceInfo.clusterId)) {
        clusterIdToFaceInfos[faceInfo.clusterId]!.add(faceInfo);
      } else {
        clusterIdToFaceInfos[faceInfo.clusterId!] = [faceInfo];
      }
    }
    final Map<int, (Vector, int)> clusterIdToMeanEmbeddingAndWeight = {};
    for (final clusterId in clusterIdToFaceInfos.keys) {
      final List<Vector> embeddings = clusterIdToFaceInfos[clusterId]!
          .map((faceInfo) => faceInfo.vEmbedding!)
          .toList();
      final count = clusterIdToFaceInfos[clusterId]!.length;
      final Vector meanEmbedding = embeddings.reduce((a, b) => a + b) / count;
      clusterIdToMeanEmbeddingAndWeight[clusterId] = (meanEmbedding, count);
    }

    // Now merge the clusters that are close to each other, based on mean embedding
    final List<(int, int)> mergedClustersList = [];
    final List<int> clusterIds =
        clusterIdToMeanEmbeddingAndWeight.keys.toList();
    log(' [CompleteClustering] ${DateTime.now()} ${clusterIds.length} clusters found, now checking for merges');
    while (true) {
      if (clusterIds.length < 2) break;
      double distance = double.infinity;
      (int, int) clusterIDsToMerge = (-1, -1);
      for (int i = 0; i < clusterIds.length; i++) {
        for (int j = 0; j < clusterIds.length; j++) {
          if (i == j) continue;
          final double newDistance = 1.0 -
              clusterIdToMeanEmbeddingAndWeight[clusterIds[i]]!.$1.dot(
                    clusterIdToMeanEmbeddingAndWeight[clusterIds[j]]!.$1,
                  );
          if (newDistance < distance) {
            distance = newDistance;
            clusterIDsToMerge = (clusterIds[i], clusterIds[j]);
          }
        }
      }
      if (distance < mergeThreshold) {
        mergedClustersList.add(clusterIDsToMerge);
        final clusterID1 = clusterIDsToMerge.$1;
        final clusterID2 = clusterIDsToMerge.$2;
        final mean1 = clusterIdToMeanEmbeddingAndWeight[clusterID1]!.$1;
        final mean2 = clusterIdToMeanEmbeddingAndWeight[clusterID2]!.$1;
        final count1 = clusterIdToMeanEmbeddingAndWeight[clusterID1]!.$2;
        final count2 = clusterIdToMeanEmbeddingAndWeight[clusterID2]!.$2;
        final weight1 = count1 / (count1 + count2);
        final weight2 = count2 / (count1 + count2);
        clusterIdToMeanEmbeddingAndWeight[clusterID1] = (
          mean1 * weight1 + mean2 * weight2,
          count1 + count2,
        );
        clusterIdToMeanEmbeddingAndWeight.remove(clusterID2);
        clusterIds.remove(clusterID2);
      } else {
        break;
      }
    }
    log(' [CompleteClustering] ${DateTime.now()} ${mergedClustersList.length} clusters merged');

    // Now assign the new clusterId to the faces
    for (final faceInfo in faceInfos) {
      for (final mergedClusters in mergedClustersList) {
        if (faceInfo.clusterId == mergedClusters.$2) {
          faceInfo.clusterId = mergedClusters.$1;
        }
      }
    }

    // Finally, assign the new clusterId to the faces
    for (final faceInfo in faceInfos) {
      newFaceIdToCluster[faceInfo.faceID] = faceInfo.clusterId!;
    }

    stopwatchClustering.stop();
    log(
      ' [CompleteClustering] ${DateTime.now()} Clustering for ${faceInfos.length} embeddings executed in ${stopwatchClustering.elapsedMilliseconds}ms',
    );

    return newFaceIdToCluster;
  }

  static List<List<String>> _runDbscanClustering(Map args) {
    final input = args['input'] as Map<String, Uint8List>;
    final fileIDToCreationTime = args['fileIDToCreationTime'] as Map<int, int>?;
    final eps = args['eps'] as double;
    final minPts = args['minPts'] as int;

    log(
      "[ClusterIsolate] ${DateTime.now()} Copied to isolate ${input.length} faces",
    );

    final DBSCAN dbscan = DBSCAN(
      epsilon: eps,
      minPoints: minPts,
      distanceMeasure: cosineDistForNormVectors,
    );

    // Organize everything into a list of FaceInfo objects
    final List<FaceInfo> faceInfos = [];
    for (final entry in input.entries) {
      faceInfos.add(
        FaceInfo(
          faceID: entry.key,
          embedding: EVector.fromBuffer(entry.value).values,
          fileCreationTime:
              fileIDToCreationTime?[getFileIdFromFaceId(entry.key)],
        ),
      );
    }

    // Sort the faceInfos based on fileCreationTime, in ascending order, so oldest faces are first
    if (fileIDToCreationTime != null) {
      faceInfos.sort((a, b) {
        if (a.fileCreationTime == null && b.fileCreationTime == null) {
          return 0;
        } else if (a.fileCreationTime == null) {
          return 1;
        } else if (b.fileCreationTime == null) {
          return -1;
        } else {
          return a.fileCreationTime!.compareTo(b.fileCreationTime!);
        }
      });
    }

    // Get the embeddings
    final List<List<double>> embeddings =
        faceInfos.map((faceInfo) => faceInfo.embedding!).toList();

    // Run the DBSCAN clustering
    final List<List<int>> clusterOutput = dbscan.run(embeddings);
    final List<List<FaceInfo>> clusteredFaceInfos = clusterOutput
        .map((cluster) => cluster.map((idx) => faceInfos[idx]).toList())
        .toList();
    final List<List<String>> clusteredFaceIDs = clusterOutput
        .map((cluster) => cluster.map((idx) => faceInfos[idx].faceID).toList())
        .toList();

    return clusteredFaceIDs;
  }
}
