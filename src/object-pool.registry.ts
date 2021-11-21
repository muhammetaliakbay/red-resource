import { Injectable } from "@nestjs/common";
import { ObjectPool } from ".";

@Injectable()
export class ObjectPoolRegistry {
    private pools: ObjectPool[] = []

    add(...pools: ObjectPool[]) {
        this.pools.push(
            ...pools,
        )
    }

    get(poolName: string): ObjectPool | null {
        return this.pools.find(
            pool => pool.client.pool === poolName
        ) ?? null
    }
}