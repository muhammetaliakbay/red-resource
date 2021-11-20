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

export class Claim {
    private promise: Promise<any> = Promise.resolve()
    private timeout: NodeJS.Timeout
    private state: 'claimed' | 'extending' | 'releasing' | 'released' | 'requeuing' | 'requeued' | 'unclaimed'

    constructor(
        readonly object: string,
        readonly session: string,
        readonly pool: ObjectPool,
    ) {
        this.setState('claimed')
    }

    private block<T>(task: () => Promise<T> | T): Promise<T> {
        const promise = this.promise.then(task)
        this.promise = promise.catch( () => {} )
        return promise
    }

    private setState(state: typeof this.state) {
        this.state = state
        if (state === 'claimed') {
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
    }

    release(): Promise<boolean> {
        return this.block(
            async () => {
                if (this.state != 'claimed') {
                    return false
                }
                this.setState('releasing')
                const result = await retry(
                    () => this.pool.client.release(
                        this.object,
                        this.session,
                    )
                )
                if (result) {
                    this.setState('released')
                } else {
                    this.setState('unclaimed')
                }
                return result
            }
        )
    }

    requeue(): Promise<boolean> {
        return this.block(
            async () => {
                if (this.state != 'claimed') {
                    return false
                }
                this.setState('requeuing')
                const result = await retry(
                    () => this.pool.client.requeue(
                        this.object,
                        this.session,
                    )
                )
                if (result) {
                    this.setState('requeued')
                } else {
                    this.setState('unclaimed')
                }
                return result
            }
        )
    }

    extend(): Promise<boolean> {
        return this.block(
            async () => {
                if (this.state != 'claimed') {
                    return false
                }
                this.setState('extending')
                const result = await retry(
                    () => this.pool.client.extend(
                        this.object,
                        this.session,
                        claimTTLSeconds,
                    )
                )
                if (result) {
                    this.setState('claimed')
                } else {
                    this.setState('unclaimed')
                }
                return result
            }
        )
    }
}

export class ObjectPool {
    constructor(
        readonly client: ObjectPoolClient,
    ) {
    }

    queue(...objects: string[]): Promise<string[]> {
        return this.client.queue(...objects)
    }

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
}
