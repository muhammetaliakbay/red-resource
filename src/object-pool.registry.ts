import { Injectable } from "@nestjs/common";
import { concat, defer, from, mergeMap, share, Subject } from "rxjs";
import { ObjectPool } from "./object-pool";

@Injectable()
export class ObjectPoolRegistry {
    private pools: ObjectPool[] = []

    private $newPools = new Subject<ObjectPool>()
    private $pools = concat(
        defer(() => from(this.pools)),
        this.$newPools,
    )

    readonly $clean = this.$pools.pipe(
        mergeMap(
            pool => pool.$clean,
        ),
        share(),
    )

    add(...pools: ObjectPool[]) {
        this.pools.push(
            ...pools,
        )
        for (const pool of pools) {
            this.$newPools.next(pool)
        }
    }

    get(poolName: string): ObjectPool | null {
        return this.pools.find(
            pool => pool.client.pool === poolName
        ) ?? null
    }
}