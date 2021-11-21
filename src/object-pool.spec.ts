import { v4 as uuidV4 } from "uuid"
import * as Redis from "ioredis"
import { expect } from "chai"
import { RedisClient } from "./redis"
import { ObjectPoolClient } from "./object-pool-client"
import { Claim, ObjectPool } from "./object-pool"

const REDIS_HOST = process.env.REDIS_HOST || 'localhost'
const REDIS_PORT = parseInt(process.env.REDIS_PORT, 10) || 6379

describe('ObjectPool', () => {
    let redis: RedisClient
    let pool: ObjectPool

    beforeEach(
        () => {
            redis = new Redis({
                host: REDIS_HOST,
                port: REDIS_PORT,
            })
            const poolName = uuidV4()
            const client = new ObjectPoolClient(
                redis,
                poolName,
            )
            pool = new ObjectPool(
                client,
            )
        }
    )
    afterEach(
        () => {
            redis.disconnect()
        }
    )

    async function forceExpire(claim: Claim) {
        const expiredSessions = await Promise.all(
            claim.objects.map(
                object =>
                    redis.getdel(pool.client.keyObjectSession(object))
            )
        )
        expect(expiredSessions).to.deep.equal(
            claim.objects.map(
                () => claim.session,
            ),
        )
    }
})
