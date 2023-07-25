/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.sdk.metrics.internal.state;

import static java.util.stream.Collectors.toMap;

import io.opentelemetry.api.internal.GuardedBy;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.descriptor.InstrumentDescriptor;
import io.opentelemetry.sdk.metrics.internal.export.RegisteredReader;
import io.opentelemetry.sdk.metrics.internal.view.RegisteredView;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * State for a {@code Meter}.
 *
 * <p>This class is internal and is hence not for public use. Its APIs are unstable and can change
 * at any time.
 */
public class MeterSharedState {

  private static final Logger logger = Logger.getLogger(MeterSharedState.class.getName());
  private final Object collectLock = new Object();
  private final Object callbackLock = new Object();

  @GuardedBy("callbackLock")
  private final List<CallbackRegistration> callbackRegistrations = new ArrayList<>();

  private final Map<RegisteredReader, MetricStorageRegistry> readerStorageRegistries;

  private final InstrumentationScopeInfo instrumentationScopeInfo;

  private MeterSharedState(
      InstrumentationScopeInfo instrumentationScopeInfo, List<RegisteredReader> registeredReaders) {
    this.instrumentationScopeInfo = instrumentationScopeInfo;
    this.readerStorageRegistries =
        registeredReaders.stream()
            .collect(toMap(Function.identity(), unused -> new MetricStorageRegistry()));
  }

  public static MeterSharedState create(
      InstrumentationScopeInfo instrumentationScopeInfo, List<RegisteredReader> registeredReaders) {
    return new MeterSharedState(instrumentationScopeInfo, registeredReaders);
  }

  /**
   * Unregister the callback.
   *
   * <p>Callbacks are originally registered via {@link #registerCallback(CallbackRegistration)}.
   */
  public void removeCallback(CallbackRegistration callbackRegistration) {
    synchronized (callbackLock) {
      this.callbackRegistrations.remove(callbackRegistration);
    }
  }

  /**
   * Register the callback.
   *
   * <p>The callback will be invoked once per collection until unregistered via {@link
   * #removeCallback(CallbackRegistration)}.
   */
  public final void registerCallback(CallbackRegistration callbackRegistration) {
    synchronized (callbackLock) {
      logger.log(
          Level.INFO, "Registering Callback :" + Integer.toString(callbackRegistrations.size()));
      callbackRegistrations.add(callbackRegistration);
    }
  }

  // only visible for testing.
  /** Returns the {@link InstrumentationScopeInfo} for this {@code Meter}. */
  public InstrumentationScopeInfo getInstrumentationScopeInfo() {
    return instrumentationScopeInfo;
  }

  /** Collects all metrics. */
  public List<MetricData> collectAll(
      RegisteredReader registeredReader,
      MeterProviderSharedState meterProviderSharedState,
      long epochNanos) {
    List<CallbackRegistration> currentRegisteredCallbacks;
    synchronized (callbackLock) {
      logger.log(
          Level.INFO,
          "Elements in callbackRegistrations :" + Integer.toString(callbackRegistrations.size()));
      currentRegisteredCallbacks = new ArrayList<>(callbackRegistrations);
    }
    // Collections across all readers are sequential
    synchronized (collectLock) {
      for (CallbackRegistration callbackRegistration : currentRegisteredCallbacks) {
        callbackRegistration.invokeCallback(
            registeredReader, meterProviderSharedState.getStartEpochNanos(), epochNanos);
      }

      Collection<MetricStorage> storages =
          Objects.requireNonNull(readerStorageRegistries.get(registeredReader)).getStorages();
      List<MetricData> result = new ArrayList<>(storages.size());
      for (MetricStorage storage : storages) {
        MetricData current =
            storage.collect(
                meterProviderSharedState.getResource(),
                getInstrumentationScopeInfo(),
                meterProviderSharedState.getStartEpochNanos(),
                epochNanos);
        // Ignore if the metric data doesn't have any data points, for example when aggregation is
        // Aggregation#drop()
        if (!current.isEmpty()) {
          result.add(current);
        }
      }
      return result;
    }
  }

  /** Reset the meter state, clearing all registered callbacks and storages. */
  public void resetForTest() {
    logger.log(Level.INFO, "resetForTest");
    synchronized (collectLock) {
      synchronized (callbackLock) {
        callbackRegistrations.clear();
      }
      this.readerStorageRegistries.values().forEach(MetricStorageRegistry::resetForTest);
    }
  }

  /** Registers new synchronous storage associated with a given instrument. */
  public final WriteableMetricStorage registerSynchronousMetricStorage(
      InstrumentDescriptor instrument, MeterProviderSharedState meterProviderSharedState) {

    List<SynchronousMetricStorage> registeredStorages = new ArrayList<>();
    for (Map.Entry<RegisteredReader, MetricStorageRegistry> entry :
        readerStorageRegistries.entrySet()) {
      RegisteredReader reader = entry.getKey();
      MetricStorageRegistry registry = entry.getValue();
      for (RegisteredView registeredView :
          reader.getViewRegistry().findViews(instrument, getInstrumentationScopeInfo())) {
        if (Aggregation.drop() == registeredView.getView().getAggregation()) {
          continue;
        }
        registeredStorages.add(
            registry.register(
                SynchronousMetricStorage.create(
                    reader,
                    registeredView,
                    instrument,
                    meterProviderSharedState.getExemplarFilter())));
      }
    }

    if (registeredStorages.size() == 1) {
      return registeredStorages.get(0);
    }

    return new MultiWritableMetricStorage(registeredStorages);
  }

  /** Register new asynchronous storage associated with a given instrument. */
  public final SdkObservableMeasurement registerObservableMeasurement(
      InstrumentDescriptor instrumentDescriptor) {
    logger.log(Level.INFO, "calling registerObservableMeasurement");
    List<AsynchronousMetricStorage<?, ?>> registeredStorages = new ArrayList<>();
    for (Map.Entry<RegisteredReader, MetricStorageRegistry> entry :
        readerStorageRegistries.entrySet()) {
      RegisteredReader reader = entry.getKey();
      MetricStorageRegistry registry = entry.getValue();
      int value = 0;
      for (RegisteredView registeredView :
          reader.getViewRegistry().findViews(instrumentDescriptor, getInstrumentationScopeInfo())) {
        logger.log(Level.INFO, "Number of views: " + value);
        value++;
        logger.log(
            Level.INFO,
            "num of views: "
                + Integer.toString(
                    reader
                        .getViewRegistry()
                        .findViews(instrumentDescriptor, getInstrumentationScopeInfo())
                        .size()));
        if (Aggregation.drop() == registeredView.getView().getAggregation()) {
          continue;
        }
        logger.log(Level.INFO, "toString : " + registeredView.getView().toString());
        registeredStorages.add(
            registry.register(
                AsynchronousMetricStorage.create(reader, registeredView, instrumentDescriptor)));
      }
    }

    return SdkObservableMeasurement.create(
        instrumentationScopeInfo, instrumentDescriptor, registeredStorages);
  }
}
