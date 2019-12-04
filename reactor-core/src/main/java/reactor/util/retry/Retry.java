/*
 * Copyright (c) 2011-Present Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.util.retry;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.annotation.Nullable;

/**
 * @author Simon Baslé
 */
public class Retry {

	static final Duration MAX_BACKOFF = Duration.ofMillis(Long.MAX_VALUE);

	public interface State {
		long failureTotalIndex();
		long failureSubsequentIndex();
		@Nullable
		Throwable failure();
	}

	public static Builder backoff(long max, Duration minBackoff) {
		return new Builder(max, t -> true, false, minBackoff, MAX_BACKOFF, 0.5d, Schedulers.parallel());
	}

	public static Builder max(int max) {
		return new Builder(max, t -> true, false, Duration.ZERO, MAX_BACKOFF, 0d, null);
	}

	/**
	 * A builder for a retry strategy with fine grained options.
	 * <p>
	 * By default the strategy is simple: rrors that match the {@link #throwablePredicate(Predicate)}
	 * (by default all) are retried up to {@link #maxAttempts(long)} times.
	 * <p>
	 * If one of the {@link #minBackoff(Duration)}, {@link #maxBackoff(Duration)}, {@link #jitter(double)}
	 * or {@link #scheduler(Scheduler)} method is used, the strategy becomes an exponential backoff strategy,
	 * randomized with a user-provided {@link #jitter(double)} factor between {@code 0.d} (no jitter)
	 * and {@code 1.0} (default is {@code 0.5}).
	 * Even with the jitter, the effective backoff delay cannot be less than {@link #minBackoff(Duration)}
	 * nor more than {@link #maxBackoff(Duration)}. The delays and subsequent attempts are executed on the
	 * provided backoff {@link #scheduler(Scheduler)}.
	 * <p>
	 * Additionally, to help dealing with bursts of transient errors in a long-lived Flux as if each burst
	 * had its own backoff, one can choose to set {@link #transientErrors(boolean)} to {@code true}.
	 * The comparison to {@link #maxAttempts(long)} will then be done with the number of subsequent attempts
	 * that failed without an {@link org.reactivestreams.Subscriber#onNext(Object) onNext} in between.
	 */
	public static class Builder {

		final Duration minBackoff;
		final Duration maxBackoff;
		final double jitterFactor;
		@Nullable
		final Scheduler backoffScheduler;

		final long                         maxAttempts;
		final Predicate<? super Throwable> throwablePredicate;
		final boolean                      isTransientErrors;

		/**
		 * Copy constructor.
		 * @param max
		 * @param minBackoff
		 * @param maxBackoff
		 * @param jitterFactor
		 * @param throwablePredicate
		 * @param backoffScheduler
		 */
		Builder(long max,
				Predicate<? super Throwable> throwablePredicate,
				boolean isTransientErrors,
				Duration minBackoff, Duration maxBackoff, double jitterFactor,
				@Nullable Scheduler backoffScheduler) {
			this.maxAttempts = max;
			this.throwablePredicate = throwablePredicate;
			this.isTransientErrors = isTransientErrors;
			this.minBackoff = minBackoff;
			this.maxBackoff = maxBackoff;
			this.jitterFactor = jitterFactor;
			this.backoffScheduler = backoffScheduler;
		}

		public Builder maxAttempts(long maxAttempts) {
			return new Builder(
					maxAttempts,
					this.throwablePredicate,
					this.isTransientErrors,
					this.minBackoff,
					this.maxBackoff,
					this.jitterFactor,
					this.backoffScheduler);
		}

		public Builder throwablePredicate(Predicate<? super Throwable> predicate) {
			return new Builder(
					this.maxAttempts,
					predicate,
					this.isTransientErrors,
					this.minBackoff,
					this.maxBackoff,
					this.jitterFactor,
					this.backoffScheduler);
		}

		public Builder throwablePredicate(
				Function<Predicate<? super Throwable>, Predicate<? super Throwable>> predicateAdjuster) {
			return new Builder(
					this.maxAttempts,
					predicateAdjuster.apply(throwablePredicate),
					this.isTransientErrors,
					this.minBackoff,
					this.maxBackoff,
					this.jitterFactor,
					this.backoffScheduler);
		}

		public Builder transientErrors(boolean isTransientErrors) {
			return new Builder(
					this.maxAttempts,
					this.throwablePredicate,
					isTransientErrors,
					this.minBackoff,
					this.maxBackoff,
					this.jitterFactor,
					this.backoffScheduler);
		}

		//all backoff specific methods should set the default scheduler if needed

		public Builder minBackoff(Duration minBackoff) {
			return new Builder(
					this.maxAttempts,
					this.throwablePredicate,
					this.isTransientErrors,
					minBackoff,
					this.maxBackoff,
					this.jitterFactor,
					this.backoffScheduler == null ? Schedulers.parallel() : this.backoffScheduler);
		}

		public Builder maxBackoff(Duration maxBackoff) {
			return new Builder(
					this.maxAttempts,
					this.throwablePredicate,
					this.isTransientErrors,
					this.minBackoff,
					maxBackoff,
					this.jitterFactor,
					this.backoffScheduler == null ? Schedulers.parallel() : this.backoffScheduler);
		}

		public Builder jitter(double jitterFactor) {
			return new Builder(
					this.maxAttempts,
					this.throwablePredicate,
					this.isTransientErrors,
					this.minBackoff,
					this.maxBackoff,
					jitterFactor,
					this.backoffScheduler == null ? Schedulers.parallel() : this.backoffScheduler);
		}

		public Builder scheduler(Scheduler backoffScheduler) {
			return new Builder(
					this.maxAttempts,
					this.throwablePredicate,
					this.isTransientErrors,
					this.minBackoff,
					this.maxBackoff,
					this.jitterFactor,
					backoffScheduler);
		}

		public Function<Flux<State>, Publisher<?>> build() {
			if (minBackoff == Duration.ZERO && maxBackoff == MAX_BACKOFF && jitterFactor == 0d && backoffScheduler == null) {
				return new SimpleRetryFunction(this);
			}
			return new ExponentialBackoffFunction(this);
		}
	}
}
