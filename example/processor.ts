import { Controller, Logger } from "@nestjs/common";
import { ObjectClaim, ObjectProcessor, PoolObject, Claim } from "../src";

@Controller()
export class Processor {
    private readonly logger = new Logger('Processor');

    @ObjectProcessor({
        pool: 'test',
        maxClaimCount: 4,
    })
    async process(
        @PoolObject object: string,
        @ObjectClaim claim: Claim,
    ) {
        this.logger.log(`Claimed an object to process, in between 3-8 seconds: ${object}`)
        await new Promise(resolve => setTimeout(resolve, (Math.random() * 5_000) + 3_000))
        this.logger.log(`Releasing the object after processing: ${object}`)
        await claim.release()
    }
}
