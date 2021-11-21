import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { DiscoveryService, MetadataScanner } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { Observable } from 'rxjs';
import { Claim, isTerminalClaimState } from './object-pool';
import { ObjectPoolRegistry } from './object-pool.registry';
import { getOptions, getParameters, setArgs } from './processor.decorator';

@Injectable()
export class ObjectProcessorExplorer implements OnApplicationBootstrap, OnApplicationShutdown {
    private readonly logger = new Logger('ObjectPool');

    constructor(
        private readonly discoveryService: DiscoveryService,
        private readonly metadataScanner: MetadataScanner,
        private readonly objectPoolRegistry: ObjectPoolRegistry,
    ) {}

    onApplicationBootstrap() {
        this.explore();
    }

    private teardownList: Function[] = []

    onApplicationShutdown() {
        const teardownList = this.teardownList.splice(0, this.teardownList.length)
        for (const teardown of teardownList) {
            teardown()
        }
    }

    explore() {
        const instanceWrappers = [
            ...this.discoveryService.getControllers(),
            ...this.discoveryService.getProviders(),
        ];
        for (const wrapper of instanceWrappers) {
            const { instance } = wrapper;
            if (!instance || !Object.getPrototypeOf(instance)) {
                return;
            }
            this.metadataScanner.scanFromPrototype(
                instance,
                Object.getPrototypeOf(instance),
                name =>
                    this.lookupProcessors(wrapper, instance, name),
            );
        }
    }

    lookupProcessors(
        wrapper: InstanceWrapper<any>,
        instance: Record<string, Function>,
        key: string
    ) {
        const options = getOptions(instance, key);

        if (options == null) {
            return
        }

        const pool = this.objectPoolRegistry.get(
            options.pool,
        )
        if (pool == null) {
            this.logger.warn(
                `Cannot register object processor "${wrapper.name}@${key}" because the pool is not found with the name ${JSON.stringify(options.pool)}.`,
            );
            return
        }

        let observable: Observable<Claim>;
        if ('tag' in options) {
            observable = pool.$claimTagged(options.tag, options.maxClaimCount, options.maxObjectPerClaim)
        } else {
            observable = pool.$claim(options.maxClaimCount)
        }

        const parameters = getParameters(instance, key) ?? []
        const subscription = observable.subscribe(
            async claim => {
                try {
                    const args = []
                    setArgs(args, parameters, claim)
                    await instance[key].apply(instance, args)
                } catch (err) {
                    this.logger.error(err)
                }
                if (!isTerminalClaimState(claim.state)) {
                    await claim.requeue()
                }
            }
        )
        this.teardownList.push(
            () => subscription.unsubscribe(),
        )
    }
}