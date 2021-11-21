#!/usr/bin/env node

import { NestFactory } from "@nestjs/core";
import { DevModule } from "./module";

async function bootstrap() {
    const app = await NestFactory.createApplicationContext(DevModule, {
        logger: ['log', 'error', 'warn', 'debug', 'verbose']
    })
    app.enableShutdownHooks()
    await app.init()
}

bootstrap().catch(
    err => {
        console.error(err)
        process.exit(1)
    }
)
