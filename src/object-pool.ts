import { BehaviorSubject, concat, concatMap, exhaustMap, filter, map, merge, observable, Observable, of, share, Subject, switchMap, tap, timer } from "rxjs";
import { ObjectPoolClient } from "./object-pool-client";

const claimTTLSeconds = 30

async function retry<T>(task: () => Promise<T> | T): Promise<T> {
    while(true) {
        try {
            return await task()
        } catch (err) {
            console.error(err)
            await new Promise(
                resolve => setTimeout(resolve, 1500)
            )
        }
    }
}

export enum ClaimState {
    Claimed = 'claimed',
    Extending = 'extending',
    Releasing = 'releasing',
    Released = 'released',
    Requeuing = 'requeuing',
    Requeued = 'requeued',
    Expired = 'expired',
}
export const TerminalClaimStates = [
    ClaimState.Expired,
    ClaimState.Released,
    ClaimState.Requeued,
] as const

export function isTerminalClaimState(state: ClaimState): state is typeof TerminalClaimStates[number] {
    return TerminalClaimStates.includes(state as any)
}

export class Claim {
    private promise: Promise<any> = Promise.resolve()
    private timeout: NodeJS.Timeout
    private stateSubject = new BehaviorSubject<ClaimState>(ClaimState.Claimed)
    get state(): ClaimState {
        return this.stateSubject.value
    }
    readonly $state: Observable<ClaimState> = this.stateSubject.asObservable()

    constructor(
        readonly object: string,
        readonly session: string,
        readonly pool: ObjectPool,
    ) {
    }

    private block<T>(task: () => Promise<T> | T): Promise<T> {
        const promise = this.promise.then(task)
        this.promise = promise.catch( () => {} )
        return promise
    }

    private setState(state: ClaimState) {
        if (this.state === state) {
            return
        } else if (isTerminalClaimState(this.state)) {
            throw new Error(`In terminal state (${this.state}) already. Bug.`)
        }

        if (state === ClaimState.Claimed) {
            if (this.timeout == null) {
                this.timeout = setTimeout(
                    this.extend.bind(this),
                    (claimTTLSeconds / 2) * 1000,
                )
            }
        } else {
            if (this.timeout != null) {
                clearTimeout(this.timeout)
                this.timeout = null
            }
        }

        this.stateSubject.next(state)
        if (isTerminalClaimState(state)) {
            this.stateSubject.complete()
        }
    }

    release(): Promise<boolean> {
        return this.block(
            async () => {
                if (this.state != ClaimState.Claimed) {
                    return false
                }
                this.setState(ClaimState.Releasing)
                const result = await retry(
                    () => this.pool.client.release(
                        this.object,
                        this.session,
                    )
                )
                if (result) {
                    this.setState(ClaimState.Released)
                } else {
                    this.setState(ClaimState.Expired)
                }
                return result
            }
        )
    }

    requeue(): Promise<boolean> {
        return this.block(
            async () => {
                if (this.state != ClaimState.Claimed) {
                    return false
                }
                this.setState(ClaimState.Requeuing)
                const result = await retry(
                    () => this.pool.client.requeue(
                        this.object,
                        this.session,
                    )
                )
                if (result) {
                    this.setState(ClaimState.Requeued)
                } else {
                    this.setState(ClaimState.Expired)
                }
                return result
            }
        )
    }

    extend(): Promise<boolean> {
        return this.block(
            async () => {
                if (this.state != ClaimState.Claimed) {
                    return false
                }
                this.setState(ClaimState.Extending)
                const result = await retry(
                    () => this.pool.client.extend(
                        this.object,
                        this.session,
                        claimTTLSeconds,
                    )
                )
                if (result) {
                    this.setState(ClaimState.Claimed)
                } else {
                    this.setState(ClaimState.Expired)
                }
                return result
            }
        )
    }
}

const claimSignalPeriodMS = 10_000

export class ObjectPool {
    constructor(
        readonly client: ObjectPoolClient,
    ) {
    }

    queue(...objects: string[]): Promise<string[]> {
        return this.client.queue(...objects)
    }

    clean(): Promise<string[]> {
        return this.client.clean()
    }
    readonly $clean: Observable<string[]> = timer(claimTTLSeconds * 1000).pipe(
        exhaustMap(
            () => this.clean(),
        ),
        share(),
    )

    async claim(maxCount: number = 1): Promise<Claim[]> {
        const {
            objects,
            session,
        } = await this.client.claim(maxCount, claimTTLSeconds)
        return objects.map(
            object => new Claim(
                object,
                session,
                this,
            )
        )
    }

    private $claimSignal = concat(
        of(-1),
        this.client.$hasQueued.pipe(
            switchMap(
                () => concat(
                    of(-1),
                    timer(claimSignalPeriodMS),
                )
            )
        ),
    )

    $claim(maxClaimedCount: number = 1): Observable<Claim> {
        let $hasFreeSpace = new Subject<void>()
        let claimedCount = 0
        return merge(
            this.$claimSignal,
            $hasFreeSpace,
        ).pipe(
            map(
                () => maxClaimedCount - claimedCount,
            ),
            filter(
                maxCount => maxCount > 0,
            ),
            exhaustMap(
                maxCount => this.claim(maxCount),
            ),
            concatMap(
                newClaims => newClaims,
            ),
            tap(
                claim => {
                    claimedCount ++;
                    claim.$state.subscribe({
                        complete: () => {
                            claimedCount --;
                            if (claimedCount === 0) {
                                $hasFreeSpace.next()
                            }
                        }
                    })
                }
            ),
            share(),
        )
    }
}
