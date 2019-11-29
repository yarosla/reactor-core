/*
 * Copyright (c) 2011-2018 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;
import java.util.stream.Stream;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.CorePublisher;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.scheduler.Scheduler;
import reactor.util.annotation.Nullable;
import reactor.util.context.Context;

/**
 * Retries a source when a companion sequence signals
 * an item in response to the main's error signal.
 * <p>
 * <p>If the companion sequence signals when the main source is active, the repeat
 * attempt is suppressed and any terminal signal will terminate the main source with the same signal immediately.
 *
 * @param <T> the source value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxRetryWhen<T> extends InternalFluxOperator<T, T> {

	static final Duration MAX_BACKOFF = Duration.ofMillis(Long.MAX_VALUE);

	final Function<? super Flux<Throwable>, ? extends Publisher<?>> whenSourceFactory;
	final boolean resetOnNext;

	FluxRetryWhen(Flux<? extends T> source,
							  Function<? super Flux<Throwable>, ? extends Publisher<?>> whenSourceFactory) {
		this(source, whenSourceFactory, false);
	}

	FluxRetryWhen(Flux<? extends T> source,
							  Function<? super Flux<Throwable>, ? extends Publisher<?>> whenSourceFactory,
			boolean resetOnNext) {
		super(source);
		this.whenSourceFactory = Objects.requireNonNull(whenSourceFactory, "whenSourceFactory");
		this.resetOnNext = resetOnNext;
	}

	/**
	 *
	 * @param other the companion publisher of throwables
	 * @param downstream the downstream of the main sequence
	 * @param whenSourceFactory the factory that decorates the other to generate retry triggers
	 * @param <T> the type of the main sequence
	 * @return true if the reset went well, false if an error was propagated to downstream
	 */
	static <T> boolean resetTrigger(@Nullable final RetryWhenOtherSubscriber other,
			@Nullable final CoreSubscriber<? super T> downstream,
			@Nullable final Function<? super Flux<Throwable>, ? extends Publisher<?>> whenSourceFactory) {
		if (other == null || downstream == null || whenSourceFactory == null) {
			return true;
		}
		Publisher<?> p;
		try {
			p = Objects.requireNonNull(whenSourceFactory.apply(other),
					"The whenSourceFactory returned a null Publisher");
		}
		catch (Throwable e) {
			downstream.onError(Operators.onOperatorError(e, downstream.currentContext()));
			return false;
		}
		p.subscribe(other);
		return true;
	}

	static <T> void subscribe(CoreSubscriber<? super T> s,
			Function<? super Flux<Throwable>, ? extends Publisher<?>> whenSourceFactory,
			CorePublisher<? extends T> source, boolean resetOnNext) {
		RetryWhenOtherSubscriber other = new RetryWhenOtherSubscriber();
		Subscriber<Throwable> signaller = Operators.serialize(other.completionSignal);

		signaller.onSubscribe(Operators.emptySubscription());

		CoreSubscriber<T> serial = Operators.serialize(s);

		RetryWhenMainSubscriber<T> main;
		if (resetOnNext) {
			main = new RetryWhenMainSubscriber<>(serial, signaller, source, other, whenSourceFactory);
		}
		else {
			main = new RetryWhenMainSubscriber<>(serial, signaller, source, null, null);
		}
		other.main = main;

		serial.onSubscribe(main);

		if (!resetTrigger(other, s, whenSourceFactory)) {
			return;
		}

		if (!main.cancelled) {
			source.subscribe(main);
		}
	}

	@Override
	public CoreSubscriber<? super T> subscribeOrReturn(CoreSubscriber<? super T> actual) {
		subscribe(actual, whenSourceFactory, source, resetOnNext);
		return null;
	}

	static final class RetryWhenMainSubscriber<T> extends
	                                              Operators.MultiSubscriptionSubscriber<T, T> {

		final Operators.SwapSubscription otherArbiter;

		final Subscriber<Throwable> signaller;

		final CorePublisher<? extends T> source;

		@Nullable
		final Function<? super Flux<Throwable>, ? extends Publisher<?>> whenSourceFactory;
		@Nullable
		final RetryWhenOtherSubscriber other;

		/**
		 * Should the next onNext call otherReset?
		 */
		boolean resetTriggerOnNextElement;

		Context context;

		volatile int wip;
		static final AtomicIntegerFieldUpdater<RetryWhenMainSubscriber> WIP =
		  AtomicIntegerFieldUpdater.newUpdater(RetryWhenMainSubscriber.class, "wip");

		long produced;
		
		RetryWhenMainSubscriber(CoreSubscriber<? super T> actual,
				Subscriber<Throwable> signaller,
				CorePublisher<? extends T> source,
				@Nullable RetryWhenOtherSubscriber other,
				@Nullable Function<? super Flux<Throwable>, ? extends Publisher<?>> whenSourceFactory) {
			super(actual);
			this.signaller = signaller;
			this.source = source;
			this.otherArbiter = new Operators.SwapSubscription(true);
			this.context = actual.currentContext();
			this.other = other;
			this.whenSourceFactory = whenSourceFactory;
			this.resetTriggerOnNextElement = false;
		}

		@Override
		public Context currentContext() {
			return this.context;
		}

		@Override
		public Stream<? extends Scannable> inners() {
			return Stream.of(Scannable.from(signaller), otherArbiter);
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				otherArbiter.cancel();
				super.cancel();
			}
		}

		void swap(Subscription w) {
			otherArbiter.swap(w);
		}

		@Override
		public void onNext(T t) {
			if (resetTriggerOnNextElement) {
				resetTriggerOnNextElement = false; //we don't want to reset for subsequent onNext
				if (!FluxRetryWhen.resetTrigger(other, actual, whenSourceFactory)) {
					return;
				}
			}
			actual.onNext(t);

			produced++;
		}

		@Override
		public void onError(Throwable t) {
			resetTriggerOnNextElement = true;
			long p = produced;
			if (p != 0L) {
				produced = 0;
				produced(p);
			}

			otherArbiter.request(1);

			signaller.onNext(t);
		}

		@Override
		public void onComplete() {
			otherArbiter.cancel();

			actual.onComplete();
		}

		void resubscribe(Object trigger) {
			if (WIP.getAndIncrement(this) == 0) {
				do {
					if (cancelled) {
						return;
					}

					//flow that emit a Context as a trigger for the re-subscription are
					//used to REPLACE the currentContext()
					if (trigger instanceof Context) {
						this.context = (Context) trigger;
					}

					source.subscribe(this);

				} while (WIP.decrementAndGet(this) != 0);
			}
		}

		void whenError(Throwable e) {
			super.cancel();

			actual.onError(e);
		}

		void whenComplete() {
			super.cancel();

			actual.onComplete();
		}
	}

	static final class RetryWhenOtherSubscriber extends Flux<Throwable>
	implements InnerConsumer<Object>, OptimizableOperator<Throwable, Throwable> {
		RetryWhenMainSubscriber<?> main;

		final DirectProcessor<Throwable> completionSignal = new DirectProcessor<>();

		@Override
		public Context currentContext() {
			return main.currentContext();
		}

		@Override
		@Nullable
		public Object scanUnsafe(Attr key) {
			if (key == Attr.PARENT) return main.otherArbiter;
			if (key == Attr.ACTUAL) return main;

			return null;
		}

		@Override
		public void onSubscribe(Subscription s) {
			main.swap(s);
		}

		@Override
		public void onNext(Object t) {
			main.resubscribe(t);
		}

		@Override
		public void onError(Throwable t) {
			main.whenError(t);
		}

		@Override
		public void onComplete() {
			main.whenComplete();
		}

		@Override
		public void subscribe(CoreSubscriber<? super Throwable> actual) {
			completionSignal.subscribe(actual);
		}

		@Override
		public CoreSubscriber<? super Throwable> subscribeOrReturn(CoreSubscriber<? super Throwable> actual) {
			return actual;
		}

		@Override
		public DirectProcessor<Throwable> source() {
			return completionSignal;
		}

		@Override
		public OptimizableOperator<?, ? extends Throwable> nextOptimizableSource() {
			return null;
		}
	}

	static Function<Flux<Throwable>, Publisher<Long>> randomExponentialBackoffFunction(
			long numRetries, Duration firstBackoff, Duration maxBackoff,
			double jitterFactor, Scheduler backoffScheduler) {
		if (jitterFactor < 0 || jitterFactor > 1) throw new IllegalArgumentException("jitterFactor must be between 0 and 1 (default 0.5)");
		Objects.requireNonNull(firstBackoff, "firstBackoff is required");
		Objects.requireNonNull(maxBackoff, "maxBackoff is required");
		Objects.requireNonNull(backoffScheduler, "backoffScheduler is required");

		return t -> t.index()
		             .flatMap(t2 -> {
			             long iteration = t2.getT1();

			             if (iteration >= numRetries) {
				             return Mono.<Long>error(new IllegalStateException("Retries exhausted: " + iteration + "/" + numRetries, t2.getT2()));
			             }

			             Duration nextBackoff;
			             try {
				             nextBackoff = firstBackoff.multipliedBy((long) Math.pow(2, iteration));
				             if (nextBackoff.compareTo(maxBackoff) > 0) {
					             nextBackoff = maxBackoff;
				             }
			             }
			             catch (ArithmeticException overflow) {
				             nextBackoff = maxBackoff;
			             }

			             //short-circuit delay == 0 case
			             if (nextBackoff.isZero()) {
				             return Mono.just(iteration);
			             }

			             ThreadLocalRandom random = ThreadLocalRandom.current();

			             long jitterOffset;
			             try {
				             jitterOffset = nextBackoff.multipliedBy((long) (100 * jitterFactor))
				                                       .dividedBy(100)
				                                       .toMillis();
			             }
			             catch (ArithmeticException ae) {
				             jitterOffset = Math.round(Long.MAX_VALUE * jitterFactor);
			             }
			             long lowBound = Math.max(firstBackoff.minus(nextBackoff)
			                                                  .toMillis(), -jitterOffset);
			             long highBound = Math.min(maxBackoff.minus(nextBackoff)
			                                                 .toMillis(), jitterOffset);

			             long jitter;
			             if (highBound == lowBound) {
				             if (highBound == 0) jitter = 0;
				             else jitter = random.nextLong(highBound);
			             }
			             else {
				             jitter = random.nextLong(lowBound, highBound);
			             }
			             Duration effectiveBackoff = nextBackoff.plusMillis(jitter);
			             return Mono.delay(effectiveBackoff, backoffScheduler);
		             });
	}
}
