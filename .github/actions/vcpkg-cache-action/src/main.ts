import * as core from '@actions/core'
import * as exec from '@actions/exec'
import * as path from 'path'

async function run(): Promise<void> {
    try {
        await exec.exec('pipx', ['run', 'uv', 'run', '--locked', 'main.py'], {
            cwd: path.resolve(__dirname, '..')
        })
    } catch (error) {
        core.setFailed(`${(error as any)?.message ?? error}`)
    }
}

run()
