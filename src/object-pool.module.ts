import { DynamicModule, Module } from "@nestjs/common"
import { DiscoveryModule } from "@nestjs/core";
import { ObjectPoolRegistry } from "./object-pool.registry";
import { ObjectProcessorExplorer } from "./processor.explorer";

@Module({
    imports: [
    ],
    providers: [
    ],
})
export class ObjectPoolModule {
    static forRoot(): DynamicModule {
        return {
            global: true,
            module: ObjectPoolModule,
            imports: [
                DiscoveryModule,
            ],
            providers: [
                ObjectProcessorExplorer,
                ObjectPoolRegistry,
            ],
            exports: [
                ObjectPoolRegistry,
            ],
        };
    }
}