import { v4 as uuidV4 } from "uuid"
import * as Redis from "ioredis"
import { RedisClient, ObjectPool } from "."
import { expect } from "chai"

const REDIS_HOST = process.env.REDIS_HOST || 'localhost'
const REDIS_PORT = parseInt(process.env.REDIS_PORT, 10) || 6379

describe('ObjectPool', () => {
    let redis: RedisClient
    let pool: string
    let objectPool: ObjectPool

    beforeEach(
        () => {
            redis = new Redis({
                host: REDIS_HOST,
                port: REDIS_PORT,
            })
            pool = uuidV4()
            objectPool = new ObjectPool(
                redis,
                pool,
            )
        }
    )
    afterEach(
        () => {
            redis.disconnect()
        }
    )

    async function checkState({
        all,
        queue,
        claims,
        objectSessions = {},
    }: {
        all: string[] | Set<string>,
        queue: string[] | Set<string>,
        claims: string[] | Set<string>,
        objectSessions?: {
            [object: string]: string | null,
        },
    }) {
        const allObjects = await redis.smembers(objectPool.keyAllObjects)
        const queuedObjects = await redis.smembers(objectPool.keyQueuedObjects)
        const objectQueue = await redis.lrange(objectPool.keyObjectQueue, 0, -1)
        const claimedObjects = await redis.lrange(objectPool.keyClaimedObjects, 0, -1)
        
        if (!(all instanceof Set)) {
            all = new Set<string>(all)
        }
        const queued = queue instanceof Set ? queue : new Set(queue)

        expect(allObjects).have.members([...all])
        expect(allObjects).have.length(all.size)

        expect(queuedObjects).have.members([...queued])
        expect(queuedObjects).have.length(queued.size)

        if (!(queue instanceof Set)) {
            expect(objectQueue).to.deep.equal(queue)
        }
        if (claims instanceof Set) {
            expect(claimedObjects).have.members([...claims])
            expect(claimedObjects).have.length(claims.size)
        } else {
            expect(claimedObjects).to.deep.equal(claims)
        }

        for(const [object, session] of Object.entries(objectSessions)) {
            const objectSession = await redis.get(
                objectPool.keyObjectSession(object)
            )
            expect(objectSession).to.equal(session)
        }
    }

    async function forceExpire(object: string, session: string) {
        const expiredSession = await redis.getdel(
            objectPool.keyObjectSession(object)
        )
        expect(expiredSession).to.equal(session)
    }

    async function getExpire(object: string, session: string) {
        const currentSession = await redis.get(
            objectPool.keyObjectSession(object)
        );
        expect(currentSession).to.equal(session)
        return Math.max(
            await redis.ttl(
                objectPool.keyObjectSession(object)
            ),
            0,
        )
    }
    
    describe('queue method', () => {
        it('should queue the object', async () => {
            const object = uuidV4()
    
            const result = await objectPool.queue(object)
    
            expect(result).to.deep.equal([object])

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should queue multiple objects', async () => {
            const objects = new Set<string>()
            for(let i = 0; i < 10; i ++) {
                objects.add(uuidV4())
            }
    
            const result = await objectPool.queue(...objects)
    
            expect(result).have.members([...objects])
            expect(result).have.length(objects.size)

            await checkState({
                all: objects,
                queue: objects,
                claims: [],
                objectSessions: Object.fromEntries(
                    [...objects].map(object => [object, null])
                ),
            })
        })

        it('should queue repeating objects', async () => {
            const objects = new Set<string>()
            const repeatingObjects = new Set<string>()
            for(let i = 0; i < 10; i ++) {
                const object = uuidV4()
                objects.add(object)
                if (i % 3 === 0) {
                    repeatingObjects.add(object)
                }
            }
    
            const result = await objectPool.queue(...objects, ...repeatingObjects)
    
            expect(result).have.members([...objects])
            expect(result).have.length(objects.size)

            await checkState({
                all: objects,
                queue: objects,
                claims: [],
                objectSessions: Object.fromEntries(
                    [...objects].map(object => [object, null])
                ),
            })
        })
    
        it('should do nothing for empty input', async () => {
            const result = await objectPool.queue()
    
            expect(result).to.deep.equal([])

            await checkState({
                all: [],
                queue: [],
                claims: [],
            })
        })

        it('should not queue an existing object', async () => {
            const object = uuidV4()
    
            const resultA = await objectPool.queue(object)
            const resultB = await objectPool.queue(object)

            expect(resultA).to.deep.equal([object])
            expect(resultB).to.deep.equal([])

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })
    })

    describe('claim method', () => {
        it('should claim the object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)
    
            const {
                session,
                objects: claimedObjects,
            } = await objectPool.claim(1, 60)
    
            expect(typeof session).to.equal('string')
            expect(claimedObjects).to.deep.equal([object])

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: session,
                },
            })
        })

        it('should not claim anything from empty queue', async () => {
            const {
                session,
                objects: claimedObjects,
            } = await objectPool.claim(1, 60)
    
            expect(typeof session).to.equal('string')
            expect(claimedObjects).to.deep.equal([])

            await checkState({
                all: [],
                queue: [],
                claims: [],
            })
        })

        it('should not claim already claimed object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session: sessionA,
                objects: claimedObjectsA,
            } = await objectPool.claim(1, 60)

            expect(claimedObjectsA).to.deep.equal([object])

            const {
                objects: claimedObjectsB,
            } = await objectPool.claim(1, 60)
    
            expect(claimedObjectsB).to.deep.equal([])

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: sessionA,
                },
            })
        })

        it('should not claim expired (not yet cleaned) object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session: sessionA,
                objects: claimedObjectsA,
            } = await objectPool.claim(1, 60)

            expect(claimedObjectsA).to.deep.equal([object])

            await forceExpire(object, sessionA)

            const {
                objects: claimedObjectsB,
            } = await objectPool.claim(1, 60)
    
            expect(claimedObjectsB).to.deep.equal([])

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should claim cleaned object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session: sessionA,
                objects: claimedObjectsA,
            } = await objectPool.claim(1, 60)

            expect(claimedObjectsA).to.deep.equal([object])

            await forceExpire(object, sessionA)

            await objectPool.clean()

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })

            const {
                session: sessionB,
                objects: claimedObjectsB,
            } = await objectPool.claim(1, 60)
    
            expect(claimedObjectsB).to.deep.equal([object])

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: sessionB,
                },
            })
        })

        it('should claim requeued object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session: sessionA,
                objects: claimedObjectsA,
            } = await objectPool.claim(1, 60)

            expect(claimedObjectsA).to.deep.equal([object])

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: sessionA,
                },
            })

            const requeueResult = await objectPool.requeue(object, sessionA)
            expect(requeueResult).to.be.true

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })

            const {
                session: sessionB,
                objects: claimedObjectsB,
            } = await objectPool.claim(1, 60)
    
            expect(claimedObjectsB).to.deep.equal([object])

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: sessionB,
                },
            })
        })

        it('should not claim released object', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session: sessionA,
                objects: claimedObjectsA,
            } = await objectPool.claim(1, 60)

            expect(claimedObjectsA).to.deep.equal([object])

            const releaseResult = await objectPool.release(object, sessionA)
            expect(releaseResult).to.be.true;

            const {
                objects: claimedObjectsB,
            } = await objectPool.claim(1, 60)
    
            expect(claimedObjectsB).to.deep.equal([])

            await checkState({
                all: [],
                queue: [],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })
    })

    describe('extend method', () => {
        it('should extend the claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 10)

            const expireA = await getExpire(object, session)
            expect(expireA).greaterThan(0).and.lessThanOrEqual(10)

            const result = await objectPool.extend(object, session, 60)
            expect(result).to.be.true

            const expireB = await getExpire(object, session)
            expect(expireB).greaterThan(10).and.lessThanOrEqual(60)

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: session,
                },
            })
        })

        it('should not extend expired claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 60)

            const expireA = await getExpire(object, session)
            expect(expireA).greaterThan(0).and.lessThanOrEqual(60)

            await forceExpire(object, session)

            const expireB = await getExpire(object, null)
            expect(expireB).to.equal(0)

            const result = await objectPool.extend(object, session, 60)
            expect(result).to.be.false

            const expireC = await getExpire(object, null)
            expect(expireC).to.equal(0)

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should not extend cleaned claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 60)

            const expireA = await getExpire(object, session)
            expect(expireA).greaterThan(0).and.lessThanOrEqual(60)

            await forceExpire(object, session)
            await objectPool.clean()

            const expireB = await getExpire(object, null)
            expect(expireB).to.equal(0)

            const result = await objectPool.extend(object, session, 60)
            expect(result).to.be.false

            const expireC = await getExpire(object, null)
            expect(expireC).to.equal(0)

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should not extend released claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 60)

            const expireA = await getExpire(object, session)
            expect(expireA).greaterThan(0).and.lessThanOrEqual(60)

            const releaseResult = await objectPool.release(object, session)
            expect(releaseResult).to.be.true

            const result = await objectPool.extend(object, session, 60)
            expect(result).to.be.false

            const expireC = await getExpire(object, null)
            expect(expireC).to.equal(0)

            await checkState({
                all: [],
                queue: [],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })
    })

    describe('release method', () => {
        it('should release the claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 60)

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: session,
                },
            })

            const result = await objectPool.release(object, session)
            expect(result).to.be.true

            await checkState({
                all: [],
                queue: [],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should not release expired claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 60)

            const expireA = await getExpire(object, session)
            expect(expireA).greaterThan(0).and.lessThanOrEqual(60)

            await forceExpire(object, session)

            const expireB = await getExpire(object, null)
            expect(expireB).to.equal(0)

            const result = await objectPool.release(object, session)
            expect(result).to.be.false

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should not release cleaned claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 60)

            const expireA = await getExpire(object, session)
            expect(expireA).greaterThan(0).and.lessThanOrEqual(60)

            await forceExpire(object, session)
            await objectPool.clean()

            const expireB = await getExpire(object, null)
            expect(expireB).to.equal(0)

            const result = await objectPool.release(object, session)
            expect(result).to.be.false

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })
    })

    describe('requeue method', () => {
        it('should requeue the claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 60)

            const result = await objectPool.requeue(object, session)
            expect(result).to.be.true

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should requeue extended claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 10)

            const expireA = await getExpire(object, session)
            expect(expireA).greaterThan(0).and.lessThanOrEqual(10)

            await objectPool.extend(object, session, 60)

            const expireB = await getExpire(object, session)
            expect(expireB).greaterThan(0).and.lessThanOrEqual(60)

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: session,
                },
            })

            const result = await objectPool.requeue(object, session)
            expect(result).to.be.true

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should not requeue expired claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 60)

            const expireA = await getExpire(object, session)
            expect(expireA).greaterThan(0).and.lessThanOrEqual(60)

            await forceExpire(object, session)

            const expireB = await getExpire(object, null)
            expect(expireB).to.equal(0)

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: null,
                },
            })

            const result = await objectPool.requeue(object, session)
            expect(result).to.be.false

            await checkState({
                all: [object],
                queue: [],
                claims: [object],
                objectSessions: {
                    [object]: null,
                },
            })
        })

        it('should not requeue cleaned claim', async () => {
            const object = uuidV4()

            await objectPool.queue(object)

            const {
                session,
            } = await objectPool.claim(1, 60)

            const expireA = await getExpire(object, session)
            expect(expireA).greaterThan(0).and.lessThanOrEqual(60)

            await forceExpire(object, session)
            await objectPool.clean()

            const expireB = await getExpire(object, null)
            expect(expireB).to.equal(0)

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })

            const result = await objectPool.requeue(object, session)
            expect(result).to.be.false

            await checkState({
                all: [object],
                queue: [object],
                claims: [],
                objectSessions: {
                    [object]: null,
                },
            })
        })
    })
})
