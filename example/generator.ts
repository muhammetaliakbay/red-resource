import { Controller, Logger } from "@nestjs/common";
import { ObjectPool, InjectObjectPool } from "../src";
import { Interval } from "@nestjs/schedule"

@Controller()
export class Generator {
    private readonly logger = new Logger('Processor');

    constructor(
        @InjectObjectPool('test-1')
        readonly pool: ObjectPool,
    ) {
    }

    // @Interval(1_000)
    async generate() {
        const testTagValue = Math.floor(Math.random() * 5).toString()
        const object = `${new Date().toLocaleString()} - #${testTagValue}`
        this.logger.log(`Queuing an object to be processed: ${object}`)
        await this.pool.queueTagged({
            'test-tag': testTagValue,
        }, [object])
    }
}
