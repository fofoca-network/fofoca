import { describe, expect, test } from 'bun:test'

import { discover, libraryName, libraryPath } from './discover.ts'

/** A filesystem that has exactly these paths. */
function only(...paths: string[]): (path: string) => boolean {
  const present = new Set(paths)
  return (path) => present.has(path)
}

describe('libraryName', () => {
  test('matches what cargo emits per platform', () => {
    expect(libraryName('darwin')).toBe('libfofoca_ffi.dylib')
    expect(libraryName('linux')).toBe('libfofoca_ffi.so')
    // Cargo drops the `lib` prefix here, and only here.
    expect(libraryName('win32')).toBe('fofoca_ffi.dll')
  })
})

describe('discover', () => {
  test('finds a release build next to the walk', () => {
    const found = '/repo/target/release/libfofoca_ffi.dylib'

    expect(
      discover({
        from: ['/repo/packages/fofoca-ffi/src'],
        name: 'libfofoca_ffi.dylib',
        exists: only(found),
      }).path,
    ).toBe(found)
  })

  test('prefers release over a debug build sitting beside it', () => {
    expect(
      discover({
        from: ['/repo/packages/fofoca-ffi/src'],
        name: 'libfofoca_ffi.dylib',
        exists: only(
          '/repo/target/debug/libfofoca_ffi.dylib',
          '/repo/target/release/libfofoca_ffi.dylib',
        ),
      }).path,
    ).toBe('/repo/target/release/libfofoca_ffi.dylib')
  })

  test('takes a debug build when that is all there is', () => {
    expect(
      discover({
        from: ['/repo/packages/fofoca-ffi/src'],
        name: 'libfofoca_ffi.dylib',
        exists: only('/repo/target/debug/libfofoca_ffi.dylib'),
      }).path,
    ).toBe('/repo/target/debug/libfofoca_ffi.dylib')
  })

  test('walks up rather than looking only beside itself', () => {
    const { searched } = discover({
      from: ['/repo/packages/fofoca-ffi/src'],
      name: 'libfofoca_ffi.dylib',
      exists: () => false,
    })

    expect(searched).toContain('/repo/packages/fofoca-ffi/src/target/release/libfofoca_ffi.dylib')
    expect(searched).toContain('/repo/target/release/libfofoca_ffi.dylib')
    expect(searched).toContain('/target/release/libfofoca_ffi.dylib')
  })

  test('reports nothing found rather than guessing', () => {
    expect(
      discover({ from: ['/repo'], name: 'libfofoca_ffi.dylib', exists: () => false }).path,
    ).toBeNull()
  })

  test('does not search the same path twice when the roots overlap', () => {
    const { searched } = discover({
      from: ['/repo/a', '/repo/a'],
      name: 'libfofoca_ffi.dylib',
      exists: () => false,
    })

    expect(new Set(searched).size).toBe(searched.length)
  })
})

describe('libraryPath', () => {
  test('says how to build it, and where it looked', () => {
    let message = ''
    try {
      libraryPath({ from: ['/repo'], name: 'libfofoca_ffi.dylib', exists: () => false })
    } catch (error) {
      message = (error as Error).message
    }

    expect(message).toContain('cargo build --release -p fofoca-ffi')
    expect(message).toContain('FOFOCA_LIB=')
    expect(message).toContain('/repo/target/release/')
  })
})
