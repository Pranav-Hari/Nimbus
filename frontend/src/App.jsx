import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import './App.css'
import heroBroomImg from './assets/nimbus_hero_broomstick.jpg'
import notFoundBroomImg from './assets/nimbus_404_broomstick.jpg'

// --- Formatting Helpers ---
function formatBytes(bytes = 0) {
  if (bytes == null || Number.isNaN(bytes)) return '0 B'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`
  if (bytes < 1024 ** 3) return `${(bytes / 1024 ** 2).toFixed(2)} MB`
  return `${(bytes / 1024 ** 3).toFixed(2)} GB`
}

function formatDate(value) {
  if (!value) return '—'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function formatNumber(value, decimals = 0) {
  if (value === null || value === undefined || value === '') return '—'
  const num = Number(value)
  if (Number.isNaN(num)) return String(value)
  return num.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}

function getFileExtension(name = '') {
  if (!name || !name.includes('.')) return 'FILE'
  return name.split('.').pop().toUpperCase()
}

// --- Inline SVG Icons ---
function DotsIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
      <circle cx="12" cy="12" r="2" />
      <circle cx="19" cy="12" r="2" />
      <circle cx="5" cy="12" r="2" />
    </svg>
  )
}

function FileTypeBadge({ ext = 'FILE' }) {
  const normalized = ext.toUpperCase()
  let bg = 'var(--bg-surface-muted)'
  let color = 'var(--text-secondary)'

  if (normalized === 'CSV' || normalized === 'PARQUET') {
    bg = 'var(--color-primary-light)'
    color = 'var(--color-primary)'
  } else if (normalized === 'JSON' || normalized === 'XML') {
    bg = 'var(--color-accent-light)'
    color = '#8A6D15'
  } else if (normalized === 'PDF') {
    bg = '#FDE8E8'
    color = '#9B1C1C'
  } else if (normalized === 'JPG' || normalized === 'PNG') {
    bg = '#EDFDF5'
    color = '#0E6245'
  }

  return (
    <span className="file-format-badge" style={{ backgroundColor: bg, color }}>
      {normalized}
    </span>
  )
}

// --- Grouping Refined Zone Outputs into Runs ---
function groupRefinedObjects(objects = []) {
  const groups = []
  const groupMap = new Map()
  const otherObjects = []

  objects.forEach((obj) => {
    const name = obj.name || ''
    // Pattern 1: multiformat/<format>/<run_id>/<filename>
    const mfMatch = name.match(/^multiformat\/([^/]+)\/([^/]+)\/(.+)$/)
    if (mfMatch) {
      const [, format, runId, artifactName] = mfMatch
      if (!groupMap.has(runId)) {
        groupMap.set(runId, {
          runId,
          format: format.toUpperCase(),
          sourceObject: '',
          timestamp: obj.last_modified,
          status: 'success',
          artifacts: [],
        })
      }
      const g = groupMap.get(runId)
      g.artifacts.push({ ...obj, artifactName })
      if (!g.timestamp || new Date(obj.last_modified) > new Date(g.timestamp)) {
        g.timestamp = obj.last_modified
      }
      return
    }

    // Pattern 2: phase4-tests/... or phase3/... or phase2/...
    const testMatch = name.match(/^(phase\d+(?:-tests)?\/[^/]+)\/([^/]+)\/(.+)$/)
    if (testMatch) {
      const [, prefix, runId, artifactName] = testMatch
      const fullRunKey = `${prefix}/${runId}`
      if (!groupMap.has(fullRunKey)) {
        groupMap.set(fullRunKey, {
          runId: fullRunKey,
          format: name.includes('.parquet') ? 'PARQUET' : 'CSV',
          sourceObject: prefix,
          timestamp: obj.last_modified,
          status: 'success',
          artifacts: [],
        })
      }
      const g = groupMap.get(fullRunKey)
      g.artifacts.push({ ...obj, artifactName })
      return
    }

    // Top-level or uncategorized object
    otherObjects.push(obj)
  })

  // Convert map to array and sort newest first
  groupMap.forEach((g) => groups.push(g))
  groups.sort((a, b) => new Date(b.timestamp || 0) - new Date(a.timestamp || 0))

  return { groups, otherObjects }
}

// =============================================================
// MAIN COMPONENT: App
// =============================================================
function App() {
  // Navigation & Hash Routing State
  const [currentHash, setCurrentHash] = useState(() => window.location.hash || '#/overview')
  const [storageZone, setStorageZone] = useState(() => {
    try {
      return window.localStorage.getItem('nimbus-active-zone') || 'raw-zone'
    } catch {
      return 'raw-zone'
    }
  })
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false)
  const [storageDropdownOpen, setStorageDropdownOpen] = useState(false)
  const storageDropdownRef = useRef(null)
  const storageCloseTimerRef = useRef(null)

  // Cluster & Storage State
  const [health, setHealth] = useState(null)
  const [rawZoneObjects, setRawZoneObjects] = useState([])
  const [refinedZoneObjects, setRefinedZoneObjects] = useState([])
  const [refreshLoading, setRefreshLoading] = useState(false)
  const [bannerNotice, setBannerNotice] = useState('')
  const [bannerError, setBannerError] = useState('')

  // Search & Sort State for Storage
  const [storageQuery, setStorageQuery] = useState('')
  const [sortBy, setSortBy] = useState('date_desc')

  // Refined Zone Run Expansion & Focus
  const [expandedRuns, setExpandedRuns] = useState(new Set())
  const [focusedRunId, setFocusedRunId] = useState(null)

  // Preview Drawer State
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [drawerExpanded, setDrawerExpanded] = useState(false)
  const [drawerObject, setDrawerObject] = useState('')
  const [drawerBucket, setDrawerBucket] = useState('raw-zone')
  const [drawerLoading, setDrawerLoading] = useState(false)
  const [drawerResult, setDrawerResult] = useState(null)
  const [drawerError, setDrawerError] = useState('')

  // Multi-File Upload State
  const [uploadQueue, setUploadQueue] = useState([])
  const [pendingOverwrite, setPendingOverwrite] = useState(null)
  const [pendingDelete, setPendingDelete] = useState(null)
  const [deleteLoading, setDeleteLoading] = useState(false)

  // Dedicated Processing Page State
  const [processSelectedFile, setProcessSelectedFile] = useState('')
  const [processLoading, setProcessLoading] = useState(false)
  const [processResult, setProcessResult] = useState(null)
  const [processError, setProcessError] = useState('')
  const [processPreviewData, setProcessPreviewData] = useState(null)

  // Benchmarks & Evaluation State
  const [benchmarkDataset, setBenchmarkDataset] = useState('100k')
  const [benchmarkLoading, setBenchmarkLoading] = useState(false)
  const [benchmarkResult, setBenchmarkResult] = useState(null)
  const [benchmarkError, setBenchmarkError] = useState('')
  const [evalTab, setEvalTab] = useState('benchmarks') // 'benchmarks' | 'spark'

  // Spark / DuckDB Demonstration Fixture State
  const [sparkLoading, setSparkLoading] = useState(false)
  const [sparkResult, setSparkResult] = useState(null)
  const [sparkError, setSparkError] = useState('')
  const [duckdbQuery, setDuckdbQuery] = useState('sales_by_city')
  const [duckdbLoading, setDuckdbLoading] = useState(false)
  const [duckdbResult, setDuckdbResult] = useState(null)
  const [duckdbError, setDuckdbError] = useState('')

  // -----------------------------------------------------------
  // Hash Routing Logic & Synchronization
  // -----------------------------------------------------------
  useEffect(() => {
    function handleHashChange() {
      const h = window.location.hash || '#/overview'
      setCurrentHash(h)
      setMobileMenuOpen(false)
      setStorageDropdownOpen(false)

      // Sync zone from hash if present
      if (h.startsWith('#/storage/refined-zone')) {
        setStorageZone('refined-zone')
      } else if (h.startsWith('#/storage/raw-zone')) {
        setStorageZone('raw-zone')
      }

      // Sync focus run if present in hash query
      if (h.includes('run=')) {
        const match = h.match(/run=([^&]+)/)
        if (match && match[1]) {
          const runId = decodeURIComponent(match[1])
          setFocusedRunId(runId)
          setExpandedRuns((prev) => new Set([...prev, runId]))
        }
      }

      // Sync process preselected file
      if (h.includes('file=')) {
        const match = h.match(/file=([^&]+)/)
        if (match && match[1]) {
          const fileName = decodeURIComponent(match[1])
          setProcessSelectedFile(fileName)
        }
      }
    }

    window.addEventListener('hashchange', handleHashChange)
    handleHashChange()
    return () => window.removeEventListener('hashchange', handleHashChange)
  }, [])

  function navigate(hash) {
    window.location.hash = hash
  }

  // Active page resolution
  const activePage = useMemo(() => {
    const raw = currentHash.split('?')[0]
    if (raw === '' || raw === '#' || raw === '#/' || raw === '#/overview') return 'overview'
    if (raw.startsWith('#/storage')) return 'storage'
    if (raw.startsWith('#/processing')) return 'processing'
    if (raw.startsWith('#/evaluation')) return 'evaluation'
    return '404'
  }, [currentHash])

  // Save active zone preference
  const handleSelectZone = useCallback((zone) => {
    setStorageZone(zone)
    try {
      window.localStorage.setItem('nimbus-active-zone', zone)
    } catch {
      // LocalStorage optional
    }
  }, [])

  // Dismiss Storage dropdown on outside click or Escape key
  useEffect(() => {
    function handleClickOutside(event) {
      if (storageDropdownRef.current && !storageDropdownRef.current.contains(event.target)) {
        setStorageDropdownOpen(false)
      }
    }
    function handleKeyDown(event) {
      if (event.key === 'Escape') {
        setStorageDropdownOpen(false)
      }
    }
    if (storageDropdownOpen) {
      document.addEventListener('mousedown', handleClickOutside)
      document.addEventListener('keydown', handleKeyDown)
    }
    return () => {
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [storageDropdownOpen])

  const handleStorageMouseEnter = useCallback(() => {
    if (storageCloseTimerRef.current) {
      clearTimeout(storageCloseTimerRef.current)
      storageCloseTimerRef.current = null
    }
    setStorageDropdownOpen(true)
  }, [])

  const handleStorageMouseLeave = useCallback(() => {
    storageCloseTimerRef.current = setTimeout(() => {
      setStorageDropdownOpen(false)
    }, 180)
  }, [])

  const handleStorageBlur = useCallback((e) => {
    if (!e.currentTarget.contains(e.relatedTarget)) {
      setStorageDropdownOpen(false)
    }
  }, [])

  // -----------------------------------------------------------
  // Fetch Storage Data & Cluster Health
  // -----------------------------------------------------------
  const loadData = useCallback(async (isManual = false) => {
    if (isManual) setRefreshLoading(true)
    try {
      const [healthRes, rawRes, refRes] = await Promise.all([
        fetch('/api/health'),
        fetch('/api/buckets/raw-zone/objects'),
        fetch('/api/buckets/refined-zone/objects'),
      ])

      if (healthRes.ok) {
        const hData = await healthRes.json()
        setHealth(hData)
      }
      if (rawRes.ok) {
        const rData = await rawRes.json()
        const rObjects = rData.objects || []
        setRawZoneObjects(rObjects)
        if (!processSelectedFile && rObjects.length > 0) {
          setProcessSelectedFile(rObjects[0].name)
        }
      }
      if (refRes.ok) {
        const refData = await refRes.json()
        const refObjects = refData.objects || []
        setRefinedZoneObjects(refObjects)

        // Automatically expand the newest run by default
        const { groups } = groupRefinedObjects(refObjects)
        if (groups.length > 0) {
          setExpandedRuns((prev) => {
            if (prev.size === 0) {
              return new Set([groups[0].runId])
            }
            return prev
          })
        }
      }
    } catch (err) {
      setBannerError(err instanceof Error ? err.message : 'Unable to connect to storage service.')
    } finally {
      if (isManual) setRefreshLoading(false)
    }
  }, [processSelectedFile])

  useEffect(() => {
    let active = true
    const fetchData = async () => {
      if (active) {
        await loadData()
      }
    }
    fetchData()
    return () => {
      active = false
    }
  }, [loadData])

  // -----------------------------------------------------------
  // Preview Drawer Handler
  // -----------------------------------------------------------
  const openPreviewDrawer = useCallback(async (objectName, targetBucket) => {
    const bucket = targetBucket || storageZone || 'raw-zone'
    setDrawerObject(objectName)
    setDrawerBucket(bucket)
    setDrawerOpen(true)
    setDrawerLoading(true)
    setDrawerError('')
    setDrawerResult(null)

    try {
      const url = `/api/objects/preview?bucket=${encodeURIComponent(bucket)}&object_name=${encodeURIComponent(objectName)}`
      const res = await fetch(url)
      const rawText = await res.text()
      let data = null
      try {
        data = rawText ? JSON.parse(rawText) : null
      } catch {
        data = null
      }
      if (!res.ok) {
        throw new Error(data?.detail || data?.message || rawText || `HTTP ${res.status}`)
      }
      setDrawerResult(data)
    } catch (err) {
      setDrawerError(err instanceof Error ? err.message : 'Failed to preview object.')
    } finally {
      setDrawerLoading(false)
    }
  }, [storageZone])

  const closePreviewDrawer = useCallback(() => {
    setDrawerOpen(false)
  }, [])

  // Close drawer on Escape key
  useEffect(() => {
    function handleKeyDown(e) {
      if (e.key === 'Escape' && drawerOpen) {
        closePreviewDrawer()
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [drawerOpen, closePreviewDrawer])

  // -----------------------------------------------------------
  // Multi-File Upload Workflow
  // -----------------------------------------------------------
  async function handleFilesSelected(fileList) {
    if (!fileList || fileList.length === 0) return
    const incoming = Array.from(fileList).map((f) => ({
      id: `${f.name}_${Date.now()}_${Math.random()}`,
      file: f,
      name: f.name,
      size: f.size,
      status: 'pending', // 'pending' | 'uploading' | 'success' | 'conflict' | 'error'
      errorMsg: '',
      durationMs: null,
      overwritten: false,
    }))

    setUploadQueue((prev) => [...incoming, ...prev])

    // Process files sequentially
    for (const item of incoming) {
      const exists = rawZoneObjects.some((o) => o.name === item.name)
      if (exists) {
        setUploadQueue((prev) =>
          prev.map((q) => (q.id === item.id ? { ...q, status: 'conflict' } : q))
        )
        continue
      }
      await uploadSingleFile(item, false)
    }
  }

  async function uploadSingleFile(queueItem, overwrite = false) {
    setUploadQueue((prev) =>
      prev.map((q) => (q.id === queueItem.id ? { ...q, status: 'uploading', errorMsg: '' } : q))
    )

    try {
      const form = new FormData()
      form.append('file', queueItem.file)
      const url = `/api/buckets/raw-zone/upload${overwrite ? '?overwrite=true' : ''}`
      const response = await fetch(url, { method: 'POST', body: form })
      const resData = await response.json()

      if (!response.ok) {
        if (response.status === 409) {
          setUploadQueue((prev) =>
            prev.map((q) => (q.id === queueItem.id ? { ...q, status: 'conflict' } : q))
          )
          return
        }
        throw new Error(resData.detail || 'Upload failed.')
      }

      setUploadQueue((prev) =>
        prev.map((q) =>
          q.id === queueItem.id
            ? {
                ...q,
                status: 'success',
                durationMs: resData.upload_duration_ms,
                overwritten: resData.overwritten,
              }
            : q
        )
      )
      setBannerNotice(`Uploaded ${queueItem.name} successfully.`)
      await loadData()
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Upload failed.'
      setUploadQueue((prev) =>
        prev.map((q) => (q.id === queueItem.id ? { ...q, status: 'error', errorMsg: msg } : q))
      )
    }
  }

  // -----------------------------------------------------------
  // Object Download & Deletion
  // -----------------------------------------------------------
  function handleDownloadObject(objName, targetBucket) {
    const bucket = targetBucket || storageZone || 'raw-zone'
    const url = `/api/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(objName)}/download`
    const link = document.createElement('a')
    link.href = url
    link.download = objName.split('/').pop() || objName
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    setBannerNotice(`Downloading ${objName}…`)
  }

  function handleRequestDelete(objName, targetBucket) {
    const bucket = targetBucket || storageZone || 'raw-zone'
    setPendingDelete({ objectName: objName, bucket })
  }

  async function executeDelete() {
    if (!pendingDelete || deleteLoading) return
    setDeleteLoading(true)
    try {
      const url = `/api/buckets/${encodeURIComponent(pendingDelete.bucket)}/objects/${encodeURIComponent(pendingDelete.objectName)}`
      const res = await fetch(url, { method: 'DELETE', headers: { Accept: 'application/json' } })
      const resData = await res.json()
      if (!res.ok) {
        throw new Error(resData.detail || 'Failed to delete object.')
      }
      setBannerNotice(`Deleted ${pendingDelete.objectName} successfully.`)
      setPendingDelete(null)
      if (drawerObject === pendingDelete.objectName) {
        closePreviewDrawer()
      }
      await loadData()
    } catch (err) {
      setBannerError(err instanceof Error ? err.message : 'Deletion failed.')
    } finally {
      setDeleteLoading(false)
    }
  }

  // -----------------------------------------------------------
  // Processing Execution Workflow
  // -----------------------------------------------------------
  // Load pre-execution preview when file selection changes
  useEffect(() => {
    let cancelled = false
    async function fetchPrePreview() {
      if (!processSelectedFile || activePage !== 'processing') return
      try {
        const res = await fetch(
          `/api/objects/preview?bucket=raw-zone&object_name=${encodeURIComponent(processSelectedFile)}`
        )
        if (res.ok && !cancelled) {
          const data = await res.json()
          setProcessPreviewData(data)
        }
      } catch {
        // Non-fatal preview fetch
      }
    }
    fetchPrePreview()
    return () => {
      cancelled = true
    }
  }, [processSelectedFile, activePage])

  async function runMultiFormatProcessing() {
    if (!processSelectedFile || processLoading) return
    setProcessLoading(true)
    setProcessError('')

    try {
      const res = await fetch('/api/processing/multiformat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ bucket: 'raw-zone', object_name: processSelectedFile }),
      })
      const rawText = await res.text()
      let data = null
      try {
        data = rawText ? JSON.parse(rawText) : null
      } catch {
        data = null
      }
      if (!res.ok) {
        throw new Error(data?.detail || data?.message || rawText || `HTTP ${res.status}`)
      }

      setProcessResult(data)
      const durText = data.processing_duration_ms != null ? ` in ${data.processing_duration_ms}ms` : ''
      setBannerNotice(`Successfully processed ${data.source_object} to refined-zone${durText}. Run ID: ${data.run_id}`)
      await loadData()
    } catch (err) {
      setProcessError(err instanceof Error ? err.message : 'Processing failed.')
    } finally {
      setProcessLoading(false)
    }
  }

  function handleViewOutputs(runId) {
    handleSelectZone('refined-zone')
    navigate(`#/storage/refined-zone?run=${encodeURIComponent(runId)}`)
  }

  function handleProcessAnother() {
    setProcessResult(null)
    setProcessError('')
  }

  // -----------------------------------------------------------
  // Benchmarking & PySpark Pipeline
  // -----------------------------------------------------------
  function extractErrorMessage(data, fallback = 'Operation failed.') {
    if (!data) return fallback
    const d = data.detail != null ? data.detail : data.message
    if (typeof d === 'string') return d
    if (typeof d === 'object' && d !== null) {
      if (d.message) {
        const extra = d.result?.error || (Array.isArray(d.log_tail) && d.log_tail.filter(Boolean).slice(-3).join('; ')) || ''
        return extra ? `${d.message}: ${extra}` : d.message
      }
      if (d.result?.error) return `Spark error: ${d.result.error}`
      if (Array.isArray(d.log_tail) && d.log_tail.length) return d.log_tail.filter(Boolean).slice(-3).join('; ')
      try {
        return JSON.stringify(d)
      } catch {
        return fallback
      }
    }
    return data.message || fallback
  }

  async function runBenchmark(datasetId = benchmarkDataset) {
    if (benchmarkLoading) return
    setBenchmarkLoading(true)
    setBenchmarkError('')

    try {
      const response = await fetch('/api/benchmark/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify({ dataset: datasetId, repetitions: 5 }),
      })
      const data = await response.json()
      if (!response.ok) {
        throw new Error(extractErrorMessage(data, 'Benchmark execution failed.'))
      }
      setBenchmarkResult(data)
      setBannerNotice(`Benchmark completed successfully for ${data.dataset_name}.`)
    } catch (err) {
      setBenchmarkError(err instanceof Error ? err.message : 'Benchmark failed.')
    } finally {
      setBenchmarkLoading(false)
    }
  }

  async function runSparkPipeline() {
    if (sparkLoading) return
    setSparkLoading(true)
    setSparkError('')
    try {
      const res = await fetch('/api/processing/run', { method: 'POST', headers: { Accept: 'application/json' } })
      const data = await res.json()
      if (!res.ok) throw new Error(extractErrorMessage(data, 'Spark pipeline failed.'))
      setSparkResult(data)
      setBannerNotice(`Spark pipeline completed in ${data.duration_seconds}s. Run ID: ${data.run_id}`)
      await loadData()
    } catch (err) {
      setSparkError(err instanceof Error ? err.message : 'Spark pipeline error.')
    } finally {
      setSparkLoading(false)
    }
  }

  async function runDuckDBQuery(qKey = duckdbQuery) {
    if (duckdbLoading) return
    setDuckdbLoading(true)
    setDuckdbError('')
    try {
      const url = `/api/analytics/duckdb?query=${encodeURIComponent(qKey)}`
      const res = await fetch(url, { method: 'POST', headers: { Accept: 'application/json' } })
      const data = await res.json()
      if (!res.ok) throw new Error(extractErrorMessage(data, 'DuckDB query failed.'))
      setDuckdbResult(data)
    } catch (err) {
      setDuckdbError(err instanceof Error ? err.message : 'DuckDB query error.')
    } finally {
      setDuckdbLoading(false)
    }
  }

  // -----------------------------------------------------------
  // Filtered & Sorted Objects for Storage View
  // -----------------------------------------------------------
  const currentZoneObjects = storageZone === 'refined-zone' ? refinedZoneObjects : rawZoneObjects

  const filteredRawObjects = useMemo(() => {
    let list = rawZoneObjects.filter((o) =>
      o.name.toLowerCase().includes(storageQuery.toLowerCase())
    )
    list.sort((a, b) => {
      if (sortBy === 'name_asc') return a.name.localeCompare(b.name)
      if (sortBy === 'name_desc') return b.name.localeCompare(a.name)
      if (sortBy === 'size_desc') return (b.size || 0) - (a.size || 0)
      if (sortBy === 'size_asc') return (a.size || 0) - (b.size || 0)
      if (sortBy === 'date_asc') return new Date(a.last_modified || 0) - new Date(b.last_modified || 0)
      return new Date(b.last_modified || 0) - new Date(a.last_modified || 0) // date_desc
    })
    return list
  }, [rawZoneObjects, storageQuery, sortBy])

  const groupedRefined = useMemo(() => {
    const { groups, otherObjects } = groupRefinedObjects(refinedZoneObjects)
    // Filter groups by search query
    const filteredGroups = groups.filter((g) => {
      const matchRun = g.runId.toLowerCase().includes(storageQuery.toLowerCase())
      const matchFormat = g.format.toLowerCase().includes(storageQuery.toLowerCase())
      const matchArtifact = g.artifacts.some((a) => a.artifactName.toLowerCase().includes(storageQuery.toLowerCase()))
      return matchRun || matchFormat || matchArtifact
    })
    const filteredOther = otherObjects.filter((o) =>
      o.name.toLowerCase().includes(storageQuery.toLowerCase())
    )
    return { groups: filteredGroups, otherObjects: filteredOther }
  }, [refinedZoneObjects, storageQuery])

  // Total bytes in current view
  const currentTotalBytes = currentZoneObjects.reduce((acc, o) => acc + (o.size || 0), 0)

  // -----------------------------------------------------------
  // RENDER VIEW
  // -----------------------------------------------------------
  return (
    <div className="app-shell">
      {/* 1. Horizontal Top Navigation */}
      <header className="topbar-nav">
        <div
          className="brand-section"
          onClick={() => navigate('#/overview')}
          role="button"
          tabIndex={0}
          onKeyDown={(e) => e.key === 'Enter' && navigate('#/overview')}
        >
          <div className="brand-titles">
            <span className="brand-name">Nimbus</span>
            <span className="brand-subtitle">LOCAL DATA LAKE</span>
          </div>
        </div>

        {/* Desktop Links */}
        <nav className="main-nav-links" aria-label="Main Navigation">
          <button
            type="button"
            className={`nav-link-btn ${activePage === 'overview' ? 'active' : ''}`}
            onClick={() => navigate('#/overview')}
          >
            Overview
          </button>

          {/* Storage Dropdown */}
          <div
            ref={storageDropdownRef}
            className={`nav-dropdown-wrap ${storageDropdownOpen ? 'open' : ''}`}
            onMouseEnter={handleStorageMouseEnter}
            onMouseLeave={handleStorageMouseLeave}
            onFocus={handleStorageMouseEnter}
            onBlur={handleStorageBlur}
          >
            <button
              type="button"
              className={`nav-link-btn ${activePage === 'storage' ? 'active' : ''}`}
              aria-expanded={storageDropdownOpen}
              aria-haspopup="true"
              onClick={(e) => {
                e.preventDefault()
              }}
            >
              <span>Storage</span>
              <span className="dropdown-chevron">▼</span>
            </button>

            {storageDropdownOpen && (
              <div
                className="nav-dropdown-menu"
                role="menu"
                aria-label="Storage Zones"
                onMouseEnter={handleStorageMouseEnter}
                onMouseLeave={handleStorageMouseLeave}
              >
                <button
                  type="button"
                  role="menuitem"
                  className={`dropdown-item ${storageZone === 'raw-zone' && activePage === 'storage' ? 'active' : ''}`}
                  onClick={() => {
                    handleSelectZone('raw-zone')
                    navigate('#/storage/raw-zone')
                    setStorageDropdownOpen(false)
                  }}
                >
                  <div className="dropdown-item-header">
                    <span className="dropdown-item-title">Raw Zone</span>
                    <span className="dropdown-item-badge">{rawZoneObjects.length} files</span>
                  </div>
                  <span className="dropdown-item-desc">Immutable landing tier for original uploads</span>
                </button>

                <button
                  type="button"
                  role="menuitem"
                  className={`dropdown-item ${storageZone === 'refined-zone' && activePage === 'storage' ? 'active' : ''}`}
                  onClick={() => {
                    handleSelectZone('refined-zone')
                    navigate('#/storage/refined-zone')
                    setStorageDropdownOpen(false)
                  }}
                >
                  <div className="dropdown-item-header">
                    <span className="dropdown-item-title">Refined Zone</span>
                    <span className="dropdown-item-badge">{refinedZoneObjects.length} outputs</span>
                  </div>
                  <span className="dropdown-item-desc">Curated artifacts grouped by processing run</span>
                </button>
              </div>
            )}
          </div>

          <button
            type="button"
            className={`nav-link-btn ${activePage === 'processing' ? 'active' : ''}`}
            onClick={() => navigate('#/processing')}
          >
            Processing
          </button>
        </nav>

        {/* Right Status & Evaluation */}
        <div className="topbar-right">
          <button
            type="button"
            className={`nav-link-btn ${activePage === 'evaluation' ? 'active' : ''}`}
            onClick={() => navigate('#/evaluation/benchmarks')}
            title="Benchmarks & Demonstration Pipeline"
          >
            <span>Evaluation</span>
          </button>

          <div className="cluster-health-pill" title="MinIO Storage Cluster Status">
            <span className={`status-dot ${health?.status === 'ok' ? 'online' : ''}`} />
            <span>{health?.status === 'ok' ? 'MinIO Online' : 'Connecting'}</span>
          </div>

          <button
            type="button"
            className="mobile-menu-btn"
            onClick={() => setMobileMenuOpen((prev) => !prev)}
            aria-label="Toggle navigation menu"
          >
            ☰
          </button>
        </div>
      </header>

      {/* Mobile Drawer Menu */}
      {mobileMenuOpen && (
        <div className="nav-dropdown-menu" style={{ position: 'fixed', top: 'var(--topbar-height)', left: '16px', right: '16px' }}>
          <button
            type="button"
            className="dropdown-item"
            onClick={() => {
              navigate('#/overview')
              setMobileMenuOpen(false)
            }}
          >
            <span className="dropdown-item-title">Overview</span>
          </button>
          <button
            type="button"
            className="dropdown-item"
            onClick={() => {
              handleSelectZone('raw-zone')
              navigate('#/storage/raw-zone')
              setMobileMenuOpen(false)
            }}
          >
            <span className="dropdown-item-title">Storage — Raw Zone</span>
          </button>
          <button
            type="button"
            className="dropdown-item"
            onClick={() => {
              handleSelectZone('refined-zone')
              navigate('#/storage/refined-zone')
              setMobileMenuOpen(false)
            }}
          >
            <span className="dropdown-item-title">Storage — Refined Zone</span>
          </button>
          <button
            type="button"
            className="dropdown-item"
            onClick={() => {
              navigate('#/processing')
              setMobileMenuOpen(false)
            }}
          >
            <span className="dropdown-item-title">Processing</span>
          </button>
          <button
            type="button"
            className="dropdown-item"
            onClick={() => {
              navigate('#/evaluation/benchmarks')
              setMobileMenuOpen(false)
            }}
          >
            <span className="dropdown-item-title">Evaluation & Benchmarks</span>
          </button>
        </div>
      )}

      {/* Main Content Area */}
      <main className="main-content">
        {bannerNotice && (
          <div className="alert-banner success" role="status">
            <span>✓ {bannerNotice}</span>
            <button type="button" className="clear-search-btn" onClick={() => setBannerNotice('')}>×</button>
          </div>
        )}
        {bannerError && (
          <div className="alert-banner error" role="alert">
            <span>⚠ {bannerError}</span>
            <button type="button" className="clear-search-btn" onClick={() => setBannerError('')}>×</button>
          </div>
        )}

        {/* -------------------------------------------------------------
            PAGE: Overview
            ------------------------------------------------------------- */}
        {activePage === 'overview' && (
          <>
            <section className="hero-card">
              <div className="hero-content">
                <span className="hero-badge">
                  <span>✦</span> S3-COMPATIBLE CLOUD DATA LAKE
                </span>
                <h1 className="hero-heading">
                  Your data, <span>organized.</span>
                </h1>
                <p className="hero-desc">
                  Ingest multi-format files into immutable landing storage, preview extracted metadata in-place, and transform structured records into refined columnar assets.
                </p>
                <div className="hero-actions">
                  <label className="btn btn-primary" style={{ cursor: 'pointer' }}>
                    <span>＋</span> Upload Files
                    <input
                      type="file"
                      multiple
                      onChange={(e) => handleFilesSelected(e.target.files)}
                      style={{ display: 'none' }}
                    />
                  </label>
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => { handleSelectZone('raw-zone'); navigate('#/storage/raw-zone'); }}
                  >
                    <span>Browse Storage</span>
                    <span>→</span>
                  </button>
                </div>
              </div>

              <div className="hero-illustration-col">
                <img
                  src={heroBroomImg}
                  alt="Nimbus Floating Broomstick"
                  className="hero-broom-img floating-broom"
                />
              </div>
            </section>

            {/* Multi-File Upload Queue (if files are uploading/uploaded) */}
            {uploadQueue.length > 0 && (
              <section className="upload-queue-panel">
                <div className="queue-header">
                  <span className="queue-title">Upload Batch Queue ({uploadQueue.length} files)</span>
                  <button
                    type="button"
                    className="btn btn-secondary btn-sm"
                    onClick={() => setUploadQueue([])}
                  >
                    Clear Completed
                  </button>
                </div>
                <div className="queue-items-list">
                  {uploadQueue.map((item) => (
                    <div key={item.id} className="queue-item-row">
                      <div className="queue-item-info">
                        <FileTypeBadge ext={getFileExtension(item.name)} />
                        <div>
                          <div className="queue-item-name">{item.name}</div>
                          <div className="queue-item-size">{formatBytes(item.size)}</div>
                        </div>
                      </div>

                      <div className="queue-item-actions">
                        {item.status === 'uploading' && (
                          <span className="status-tag pending">
                            <span className="spin">↻</span> Uploading…
                          </span>
                        )}

                        {item.status === 'conflict' && (
                          <>
                            <span className="status-tag warning">File Already Exists</span>
                            <button
                              type="button"
                              className="btn btn-primary btn-sm"
                              onClick={() => uploadSingleFile(item, true)}
                            >
                              Overwrite
                            </button>
                            <button
                              type="button"
                              className="btn btn-secondary btn-sm"
                              onClick={() =>
                                setUploadQueue((prev) => prev.filter((q) => q.id !== item.id))
                              }
                            >
                              Skip
                            </button>
                          </>
                        )}

                        {item.status === 'error' && (
                          <span className="status-tag danger">{item.errorMsg || 'Upload Error'}</span>
                        )}

                        {item.status === 'success' && (
                          <>
                            <span className="status-tag success">
                              ✓ Uploaded {item.durationMs ? `(${item.durationMs}ms)` : ''}
                            </span>
                            <button
                              type="button"
                              className="btn btn-secondary btn-sm"
                              onClick={() => openPreviewDrawer(item.name, 'raw-zone')}
                            >
                              Preview
                            </button>
                            <button
                              type="button"
                              className="btn btn-primary btn-sm"
                              onClick={() => {
                                setProcessSelectedFile(item.name)
                                navigate(`#/processing?file=${encodeURIComponent(item.name)}&bucket=raw-zone`)
                              }}
                            >
                              Process File
                            </button>
                            <button
                              type="button"
                              className="btn btn-secondary btn-sm"
                              onClick={() => {
                                handleSelectZone('raw-zone')
                                navigate('#/storage/raw-zone')
                              }}
                            >
                              View in Storage
                            </button>
                          </>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </section>
            )}

            {/* Storage Zone Gateway Cards */}
            <div className="zone-gateways-grid">
              <div className="gateway-card">
                <div className="gateway-card-header">
                  <div className="gateway-icon-badge raw">▤</div>
                  <span className="gateway-stats-pill">{rawZoneObjects.length} Objects</span>
                </div>
                <h3 className="gateway-title">Raw Zone</h3>
                <p className="gateway-desc">
                  Immutable landing tier for original files. Direct uploads permitted with duplicate collision safeguards. Files remain preserved during transformations.
                </p>
                <div className="gateway-footer">
                  <span className="gateway-size-text">
                    Total: {formatBytes(rawZoneObjects.reduce((acc, o) => acc + (o.size || 0), 0))}
                  </span>
                  <button
                    type="button"
                    className="btn btn-secondary btn-sm"
                    onClick={() => { handleSelectZone('raw-zone'); navigate('#/storage/raw-zone'); }}
                  >
                    Open Raw Zone →
                  </button>
                </div>
              </div>

              <div className="gateway-card">
                <div className="gateway-card-header">
                  <div className="gateway-icon-badge refined">◈</div>
                  <span className="gateway-stats-pill">{refinedZoneObjects.length} Objects</span>
                </div>
                <h3 className="gateway-title">Refined Zone</h3>
                <p className="gateway-desc">
                  Curated storage tier for generated records and execution manifests. Populated exclusively through verified multi-format processing workflows.
                </p>
                <div className="gateway-footer">
                  <span className="gateway-size-text">
                    Total: {formatBytes(refinedZoneObjects.reduce((acc, o) => acc + (o.size || 0), 0))}
                  </span>
                  <button
                    type="button"
                    className="btn btn-secondary btn-sm"
                    onClick={() => { handleSelectZone('refined-zone'); navigate('#/storage/refined-zone'); }}
                  >
                    Open Refined Zone →
                  </button>
                </div>
              </div>
            </div>
          </>
        )}

        {/* -------------------------------------------------------------
            PAGE: Storage Browser
            ------------------------------------------------------------- */}
        {activePage === 'storage' && (
          <>
            <div className="page-header">
              <div>
                <div className="page-eyebrow">STORAGE BROWSER</div>
                <h1 className="page-title">{storageZone === 'raw-zone' ? 'Raw Zone' : 'Refined Zone'}</h1>
                <p className="page-description">
                  {storageZone === 'raw-zone'
                    ? 'Browse original uploaded objects in the immutable landing zone. Click any filename to inspect in-place.'
                    : 'Curated outputs grouped by processing run. Expanding a run reveals records and manifests.'}
                </p>
              </div>

              <div className="page-header-actions">
                {storageZone === 'raw-zone' && (
                  <label className="btn btn-primary" style={{ cursor: 'pointer' }}>
                    <span>＋</span> Upload Files
                    <input
                      type="file"
                      multiple
                      onChange={(e) => handleFilesSelected(e.target.files)}
                      style={{ display: 'none' }}
                    />
                  </label>
                )}
                <button
                  type="button"
                  className="btn btn-secondary"
                  onClick={() => loadData(true)}
                  disabled={refreshLoading}
                >
                  <span className={refreshLoading ? 'spin' : ''}>↻</span> Refresh
                </button>
              </div>
            </div>

            {/* Zone Pill Switcher */}
            <div className="zone-switcher-bar">
              <div className="zone-tabs-pills">
                <button
                  type="button"
                  className={`zone-tab-btn ${storageZone === 'raw-zone' ? 'active' : ''}`}
                  onClick={() => { handleSelectZone('raw-zone'); navigate('#/storage/raw-zone'); }}
                >
                  Raw Zone ({rawZoneObjects.length})
                </button>
                <button
                  type="button"
                  className={`zone-tab-btn ${storageZone === 'refined-zone' ? 'active' : ''}`}
                  onClick={() => { handleSelectZone('refined-zone'); navigate('#/storage/refined-zone'); }}
                >
                  Refined Zone ({refinedZoneObjects.length})
                </button>
              </div>

              <span className="gateway-size-text">
                Listed: {formatBytes(currentTotalBytes)} ({currentZoneObjects.length} objects)
              </span>
            </div>

            {/* Filter & Sort Bar */}
            <div className="table-filter-bar">
              <div className="search-input-wrap">
                <span className="search-icon-pos">⌕</span>
                <input
                  type="text"
                  className="search-input-field"
                  value={storageQuery}
                  onChange={(e) => setStorageQuery(e.target.value)}
                  placeholder="Filter by name or run ID…"
                  aria-label="Filter objects by name"
                />
                {storageQuery && (
                  <button
                    type="button"
                    className="clear-search-btn"
                    onClick={() => setStorageQuery('')}
                    aria-label="Clear filter"
                  >
                    ×
                  </button>
                )}
              </div>

              <div className="sort-select-wrap">
                <span className="sort-label">Sort:</span>
                <select
                  className="sort-select-field"
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value)}
                >
                  <option value="date_desc">Date (Newest first)</option>
                  <option value="date_asc">Date (Oldest first)</option>
                  <option value="name_asc">Name (A – Z)</option>
                  <option value="name_desc">Name (Z – A)</option>
                  <option value="size_desc">Size (Largest first)</option>
                  <option value="size_asc">Size (Smallest first)</option>
                </select>
              </div>
            </div>

            {/* Raw Zone Table */}
            {storageZone === 'raw-zone' && (
              <div className="table-card-container">
                <table className="custom-table" aria-label="Raw Zone Objects">
                  <thead>
                    <tr>
                      <th>OBJECT NAME</th>
                      <th>TYPE</th>
                      <th>SIZE</th>
                      <th>MODIFIED</th>
                      <th style={{ width: '60px', textAlign: 'center' }}>ACTIONS</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredRawObjects.length === 0 ? (
                      <tr>
                        <td colSpan={5} style={{ textAlign: 'center', padding: '36px', color: 'var(--text-muted)' }}>
                          {storageQuery ? 'No objects match your search.' : 'Raw zone is empty. Upload files above.'}
                        </td>
                      </tr>
                    ) : (
                      filteredRawObjects.map((obj) => (
                        <tr key={obj.name}>
                          <td>
                            <button
                              type="button"
                              className="file-name-btn"
                              onClick={() => openPreviewDrawer(obj.name, 'raw-zone')}
                              title={`Preview ${obj.name}`}
                            >
                              <span>{obj.name}</span>
                            </button>
                          </td>
                          <td>
                            <FileTypeBadge ext={getFileExtension(obj.name)} />
                          </td>
                          <td style={{ fontFamily: 'var(--font-mono)' }}>{formatBytes(obj.size)}</td>
                          <td style={{ color: 'var(--text-muted)', fontSize: '13px' }}>{formatDate(obj.last_modified)}</td>
                          <td style={{ textAlign: 'center' }}>
                            <RowActionsMenu
                              objectName={obj.name}
                              bucket="raw-zone"
                              onPreview={() => openPreviewDrawer(obj.name, 'raw-zone')}
                              onProcess={() => {
                                setProcessSelectedFile(obj.name)
                                navigate(`#/processing?file=${encodeURIComponent(obj.name)}&bucket=raw-zone`)
                              }}
                              onDownload={() => handleDownloadObject(obj.name, 'raw-zone')}
                              onDelete={() => handleRequestDelete(obj.name, 'raw-zone')}
                            />
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}

            {/* Refined Zone Grouped Runs View */}
            {storageZone === 'refined-zone' && (
              <div className="runs-groups-container">
                {groupedRefined.groups.length === 0 && groupedRefined.otherObjects.length === 0 && (
                  <div className="table-card-container" style={{ padding: '40px', textAlign: 'center', color: 'var(--text-muted)' }}>
                    No refined objects found. Run a processing job in the Processing tab to generate outputs.
                  </div>
                )}

                {groupedRefined.groups.map((grp) => {
                  const isExpanded = expandedRuns.has(grp.runId)
                  const isFocused = focusedRunId === grp.runId

                  return (
                    <div
                      key={grp.runId}
                      className={`run-group-card ${isFocused ? 'highlighted' : ''}`}
                    >
                      <div
                        className="run-group-header"
                        onClick={() => {
                          setExpandedRuns((prev) => {
                            const next = new Set(prev)
                            if (next.has(grp.runId)) next.delete(grp.runId)
                            else next.add(grp.runId)
                            return next
                          })
                        }}
                      >
                        <div className="run-meta-left">
                          <FileTypeBadge ext={grp.format} />
                          <span className="run-id-pill">
                            Run ID: <code>{grp.runId}</code>
                          </span>
                          <span className="status-tag success">Completed</span>
                          <span className="gateway-stats-pill">{grp.artifacts.length} outputs</span>
                        </div>

                        <div className="run-meta-right">
                          <span className="run-timestamp">{formatDate(grp.timestamp)}</span>
                          <span className={`run-chevron ${isExpanded ? 'expanded' : ''}`}>▼</span>
                        </div>
                      </div>

                      {isExpanded && (
                        <div className="run-artifacts-table-wrap">
                          <table className="custom-table">
                            <thead>
                              <tr>
                                <th>OUTPUT ARTIFACT</th>
                                <th>FULL PATH</th>
                                <th>SIZE</th>
                                <th style={{ width: '60px', textAlign: 'center' }}>ACTIONS</th>
                              </tr>
                            </thead>
                            <tbody>
                              {grp.artifacts.map((art) => (
                                <tr key={art.name}>
                                  <td>
                                    <button
                                      type="button"
                                      className="file-name-btn"
                                      onClick={() => openPreviewDrawer(art.name, 'refined-zone')}
                                      title={`Preview ${art.artifactName || art.name}`}
                                    >
                                      <strong>{art.artifactName || art.name}</strong>
                                    </button>
                                  </td>
                                  <td style={{ fontFamily: 'var(--font-mono)', fontSize: '12px', color: 'var(--text-muted)' }}>
                                    refined-zone/{art.name}
                                  </td>
                                  <td style={{ fontFamily: 'var(--font-mono)' }}>{formatBytes(art.size)}</td>
                                  <td style={{ textAlign: 'center' }}>
                                    <RowActionsMenu
                                      objectName={art.name}
                                      bucket="refined-zone"
                                      onPreview={() => openPreviewDrawer(art.name, 'refined-zone')}
                                      onDownload={() => handleDownloadObject(art.name, 'refined-zone')}
                                      onDelete={() => handleRequestDelete(art.name, 'refined-zone')}
                                    />
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  )
                })}

                {/* Legacy / Historical Outputs Card */}
                {groupedRefined.otherObjects.length > 0 && (
                  <div className="run-group-card">
                    <div
                      className="run-group-header"
                      onClick={() => {
                        setExpandedRuns((prev) => {
                          const next = new Set(prev)
                          if (next.has('other-outputs')) next.delete('other-outputs')
                          else next.add('other-outputs')
                          return next
                        })
                      }}
                    >
                      <div className="run-meta-left">
                        <span className="run-id-pill">Other Objects & Test Artifacts</span>
                        <span className="gateway-stats-pill">{groupedRefined.otherObjects.length} files</span>
                      </div>
                      <div className="run-meta-right">
                        <span className="run-chevron">{expandedRuns.has('other-outputs') ? '▲' : '▼'}</span>
                      </div>
                    </div>

                    {expandedRuns.has('other-outputs') && (
                      <div className="run-artifacts-table-wrap">
                        <table className="custom-table">
                          <thead>
                            <tr>
                              <th>OBJECT NAME</th>
                              <th>TYPE</th>
                              <th>SIZE</th>
                              <th>MODIFIED</th>
                              <th style={{ width: '60px', textAlign: 'center' }}>ACTIONS</th>
                            </tr>
                          </thead>
                          <tbody>
                            {groupedRefined.otherObjects.map((obj) => (
                              <tr key={obj.name}>
                                <td>
                                  <button
                                    type="button"
                                    className="file-name-btn"
                                    onClick={() => openPreviewDrawer(obj.name, 'refined-zone')}
                                  >
                                    {obj.name}
                                  </button>
                                </td>
                                <td>
                                  <FileTypeBadge ext={getFileExtension(obj.name)} />
                                </td>
                                <td style={{ fontFamily: 'var(--font-mono)' }}>{formatBytes(obj.size)}</td>
                                <td style={{ color: 'var(--text-muted)' }}>{formatDate(obj.last_modified)}</td>
                                <td style={{ textAlign: 'center' }}>
                                  <RowActionsMenu
                                    objectName={obj.name}
                                    bucket="refined-zone"
                                    onPreview={() => openPreviewDrawer(obj.name, 'refined-zone')}
                                    onDownload={() => handleDownloadObject(obj.name, 'refined-zone')}
                                    onDelete={() => handleRequestDelete(obj.name, 'refined-zone')}
                                  />
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                )}
              </div>
            )}
          </>
        )}

        {/* -------------------------------------------------------------
            PAGE: Processing Workflow
            ------------------------------------------------------------- */}
        {activePage === 'processing' && (
          <div className="processing-workflow-card">
            <div className="page-header" style={{ marginBottom: '24px' }}>
              <div>
                <div className="page-eyebrow">DATA PROCESSING</div>
                <h1 className="page-title">Transform & Extract</h1>
                <p className="page-description">
                  Extract structured content from raw-zone objects, inspect format schemas, and save standardized partition artifacts to refined-zone.
                </p>
              </div>
            </div>

            {/* STAGE 1: Select & Review */}
            <div className="stage-card">
              <div className="stage-header-row">
                <div>
                  <span className="stage-number-pill">Stage 1</span>
                  <h3 className="stage-title">Select Source Object</h3>
                  <div className="stage-desc">
                    Choose an existing file from raw-zone to inspect and process.
                  </div>
                </div>
                {processSelectedFile && (
                  <FileTypeBadge ext={getFileExtension(processSelectedFile)} />
                )}
              </div>

              <div className="file-picker-box">
                <div>
                  <label htmlFor="process-file-select" style={{ fontSize: '13px', fontWeight: 600, color: 'var(--text-secondary)' }}>
                    RAW-ZONE SOURCE FILE:
                  </label>
                  <div style={{ marginTop: '6px' }}>
                    <select
                      id="process-file-select"
                      className="picker-select-control"
                      value={processSelectedFile}
                      onChange={(e) => {
                        setProcessSelectedFile(e.target.value)
                        setProcessResult(null)
                      }}
                      disabled={processLoading || rawZoneObjects.length === 0}
                    >
                      {rawZoneObjects.length === 0 ? (
                        <option value="">No files in raw-zone</option>
                      ) : (
                        rawZoneObjects.map((obj) => (
                          <option key={obj.name} value={obj.name}>
                            {obj.name} ({formatBytes(obj.size)})
                          </option>
                        ))
                      )}
                    </select>
                  </div>
                </div>

                {processSelectedFile && (
                  <div style={{ display: 'flex', gap: '8px' }}>
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={() => openPreviewDrawer(processSelectedFile, 'raw-zone')}
                    >
                      Open Full Preview
                    </button>
                  </div>
                )}
              </div>

              {/* Pre-run Summary */}
              {processPreviewData && (
                <div style={{ marginTop: '16px', padding: '14px 18px', backgroundColor: 'var(--bg-surface-subtle)', borderRadius: '10px', fontSize: '13.5px' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span>
                      Detected Format: <strong>{processPreviewData.format}</strong> • Record/Page Count: <strong>{formatNumber(processPreviewData.record_or_page_count)}</strong>
                    </span>
                    <span style={{ color: 'var(--text-muted)' }}>
                      Parses content into <code>extracted_records.json</code> & <code>manifest.json</code>
                    </span>
                  </div>
                </div>
              )}
            </div>

            {/* STAGE 2: Run Processing */}
            <div className="stage-card">
              <div className="stage-header-row">
                <div>
                  <span className="stage-number-pill">Stage 2</span>
                  <h3 className="stage-title">Run Multi-Format Extraction</h3>
                  <div className="stage-desc">
                    Processes file according to its format specification and writes outputs directly to refined-zone.
                  </div>
                </div>

                <button
                  type="button"
                  className="btn btn-primary"
                  onClick={runMultiFormatProcessing}
                  disabled={processLoading || !processSelectedFile}
                >
                  <span className={processLoading ? 'spin' : ''}>
                    {processLoading ? '↻' : '▶'}
                  </span>
                  <span>{processLoading ? 'Processing Object…' : 'Run Processing'}</span>
                </button>
              </div>

              {processLoading && (
                <div className="processing-active-banner">
                  <span className="spin" style={{ fontSize: '20px', color: 'var(--color-primary)' }}>↻</span>
                  <div>
                    <strong>Processing {processSelectedFile} in progress…</strong>
                    <p style={{ margin: '4px 0 0 0', fontSize: '13px', color: 'var(--text-secondary)' }}>
                      Extracting records, cataloging metadata, and compiling execution manifest into <code>refined-zone/multiformat/</code>.
                    </p>
                  </div>
                </div>
              )}

              {processError && (
                <div className="alert-banner error" style={{ marginTop: '16px' }}>
                  <span>⚠ {processError}</span>
                </div>
              )}
            </div>

            {/* STAGE 3, 4, 5: Current Run Results */}
            {processResult && (
              <div className="run-result-card">
                <div className="run-result-header">
                  <div>
                    <span className="page-eyebrow">STAGE 3 / EXECUTION COMPLETE</span>
                    <h3 style={{ fontSize: '20px', margin: '4px 0 0 0', color: 'var(--text-main)' }}>
                      Run {processResult.run_id} Results
                    </h3>
                  </div>

                  <div className="run-result-actions">
                    <button
                      type="button"
                      className="btn btn-primary"
                      onClick={() => handleViewOutputs(processResult.run_id)}
                    >
                      View Outputs in Refined Zone →
                    </button>
                    <button
                      type="button"
                      className="btn btn-secondary"
                      onClick={handleProcessAnother}
                    >
                      Process Another File
                    </button>
                  </div>
                </div>

                {processResult.status === 'partial' && (
                  <div className="partial-alert" style={{ marginBottom: '20px' }}>
                    <span style={{ fontSize: '18px' }}>⚠</span>
                    <div>
                      <strong>Partial Extraction Notice:</strong> Scanned images or PDFs without a digital text layer were cataloged with partial text extraction. Full textual decoding requires OCR.
                    </div>
                  </div>
                )}

                <div className="result-details-grid">
                  <div className="result-detail-item">
                    <span className="result-detail-label">STATUS</span>
                    <span className="result-detail-val">
                      <span className={`status-tag ${processResult.status === 'success' ? 'success' : 'warning'}`}>
                        {processResult.status.toUpperCase()}
                      </span>
                    </span>
                  </div>
                  <div className="result-detail-item">
                    <span className="result-detail-label">DETECTED FORMAT</span>
                    <span className="result-detail-val">{processResult.format}</span>
                  </div>
                  <div className="result-detail-item">
                    <span className="result-detail-label">RECORDS / PAGES</span>
                    <span className="result-detail-val" style={{ fontFamily: 'var(--font-mono)' }}>
                      {formatNumber(processResult.record_or_page_count)}
                    </span>
                  </div>
                  <div className="result-detail-item">
                    <span className="result-detail-label">PROCESSING TIME</span>
                    <span className="result-detail-val" style={{ fontFamily: 'var(--font-mono)', color: 'var(--color-primary)' }}>
                      {processResult.processing_duration_ms} ms
                    </span>
                  </div>
                  <div className="result-detail-item" style={{ gridColumn: '1 / -1' }}>
                    <span className="result-detail-label">GENERATED OUTPUTS</span>
                    <div style={{ marginTop: '6px', fontFamily: 'var(--font-mono)', fontSize: '13px' }}>
                      <div>• refined-zone/{processResult.records_object}</div>
                      <div>• refined-zone/{processResult.manifest_object}</div>
                    </div>
                  </div>
                </div>

                {/* Extracted Preview Snippet */}
                {Array.isArray(processResult.extracted_preview) && processResult.extracted_preview.length > 0 && (
                  <div>
                    <h4 style={{ fontSize: '14px', marginBottom: '8px', color: 'var(--text-secondary)' }}>
                      Bounded Extracted Records Preview:
                    </h4>
                    <div className="drawer-table-wrap">
                      <pre className="preformatted-text-box" style={{ maxHeight: '220px' }}>
                        {JSON.stringify(processResult.extracted_preview.slice(0, 5), null, 2)}
                      </pre>
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* -------------------------------------------------------------
            PAGE: Evaluation & Benchmarks
            ------------------------------------------------------------- */}
        {activePage === 'evaluation' && (
          <div className="benchmark-card">
            <div className="page-header" style={{ marginBottom: '20px' }}>
              <div>
                <div className="page-eyebrow">PERFORMANCE EVALUATION</div>
                <h1 className="page-title">Benchmarks & Verification</h1>
                <p className="page-description">
                  Evaluate columnar storage performance and test distributed processing engines in a local S3-compatible environment.
                </p>
              </div>
            </div>

            {/* Sub-Tabs: Benchmarks vs Spark Pipeline */}
            <div className="eval-tabs-bar">
              <button
                type="button"
                className={`eval-tab-btn ${evalTab === 'benchmarks' ? 'active' : ''}`}
                onClick={() => setEvalTab('benchmarks')}
              >
                Columnar Storage Benchmarks
              </button>
              <button
                type="button"
                className={`eval-tab-btn ${evalTab === 'spark' ? 'active' : ''}`}
                onClick={() => setEvalTab('spark')}
              >
                Demonstration PySpark Pipeline
              </button>
            </div>

            {/* TAB 1: Benchmarks */}
            {evalTab === 'benchmarks' && (
              <>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '16px', flexWrap: 'wrap', marginBottom: '20px' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                    <label htmlFor="benchmark-ds" style={{ fontSize: '13px', fontWeight: 600 }}>DATASET:</label>
                    <select
                      id="benchmark-ds"
                      className="picker-select-control"
                      value={benchmarkDataset}
                      onChange={(e) => setBenchmarkDataset(e.target.value)}
                      disabled={benchmarkLoading}
                    >
                      <option value="100k">100,000-Row Synthetic Benchmark Dataset</option>
                      <option value="small">Small Production Sample Dataset (8 records)</option>
                    </select>
                  </div>

                  <button
                    type="button"
                    className="btn btn-primary"
                    onClick={() => runBenchmark(benchmarkDataset)}
                    disabled={benchmarkLoading}
                  >
                    <span className={benchmarkLoading ? 'spin' : ''}>
                      {benchmarkLoading ? '↻' : '▶'}
                    </span>
                    <span>{benchmarkLoading ? 'Running 5 Repetitions…' : 'Run Empirical Benchmark'}</span>
                  </button>
                </div>

                <div className="benchmark-disclaimer-box">
                  <strong>Evaluation Scope:</strong> Measures empirical compression and query throughput comparing row-oriented CSV against Snappy-compressed Parquet across structured tabular datasets. Columnar speedup applies to tabular records and does not represent unstructured media (PDFs, images, logs).
                </div>

                {benchmarkError && (
                  <div className="alert-banner error" style={{ marginTop: '16px' }}>
                    <span>⚠ {benchmarkError}</span>
                  </div>
                )}

                {benchmarkResult && (
                  <div style={{ marginTop: '28px' }}>
                    {/* Summary Cards */}
                    <div className="stats-summary-grid">
                      <div className="stat-box">
                        <span className="stat-box-label">STORAGE REDUCTION</span>
                        <span className="stat-box-value">{benchmarkResult.storage_metrics.size_reduction_pct}%</span>
                        <span className="stat-box-detail">
                          Parquet: {formatBytes(benchmarkResult.storage_metrics.parquet.total_bytes)} vs CSV: {formatBytes(benchmarkResult.storage_metrics.csv.total_bytes)}
                        </span>
                      </div>
                      <div className="stat-box">
                        <span className="stat-box-label">RETRIEVAL SPEEDUP</span>
                        <span className="stat-box-value">
                          {(benchmarkResult.retrieval_metrics.csv_retrieval_ms / Math.max(benchmarkResult.retrieval_metrics.parquet_retrieval_ms, 1)).toFixed(2)}x
                        </span>
                        <span className="stat-box-detail">
                          Parquet: {benchmarkResult.retrieval_metrics.parquet_retrieval_ms}ms vs CSV: {benchmarkResult.retrieval_metrics.csv_retrieval_ms}ms
                        </span>
                      </div>
                      <div className="stat-box">
                        <span className="stat-box-label">SCAN & PARSE SPEEDUP</span>
                        <span className="stat-box-value">
                          {(benchmarkResult.scan_metrics.csv_initial_scan_ms / Math.max(benchmarkResult.scan_metrics.parquet_initial_scan_ms, 1)).toFixed(2)}x
                        </span>
                        <span className="stat-box-detail">
                          Parquet: {benchmarkResult.scan_metrics.parquet_initial_scan_ms}ms vs CSV: {benchmarkResult.scan_metrics.csv_initial_scan_ms}ms
                        </span>
                      </div>
                    </div>

                    {/* Storage Bar Visual Comparison */}
                    <div style={{ marginBottom: '28px' }}>
                      <h3 style={{ fontSize: '17px', margin: '0 0 12px 0' }}>Storage Footprint Comparison</h3>
                      <div className="storage-bars-chart">
                        <div className="chart-bar-row">
                          <div className="chart-bar-meta">
                            <span>CSV ({benchmarkResult.storage_metrics.csv.part_files_count} part file)</span>
                            <span>{formatBytes(benchmarkResult.storage_metrics.csv.total_bytes)}</span>
                          </div>
                          <div className="chart-track">
                            <div className="chart-fill csv" style={{ width: '100%' }}>
                              {formatBytes(benchmarkResult.storage_metrics.csv.total_bytes)}
                            </div>
                          </div>
                        </div>

                        <div className="chart-bar-row">
                          <div className="chart-bar-meta">
                            <span>Snappy Parquet ({benchmarkResult.storage_metrics.parquet.part_files_count} part file)</span>
                            <span>{formatBytes(benchmarkResult.storage_metrics.parquet.total_bytes)}</span>
                          </div>
                          <div className="chart-track">
                            <div
                              className="chart-fill parquet"
                              style={{
                                width: `${Math.max(
                                  Math.round((benchmarkResult.storage_metrics.parquet.total_bytes / benchmarkResult.storage_metrics.csv.total_bytes) * 100),
                                  6
                                )}%`,
                              }}
                            >
                              {formatBytes(benchmarkResult.storage_metrics.parquet.total_bytes)}
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* Query Table */}
                    <h3 style={{ fontSize: '17px', margin: '0 0 12px 0' }}>Detailed Query Timings (5 Repetitions)</h3>
                    <div className="table-card-container">
                      <table className="custom-table">
                        <thead>
                          <tr>
                            <th>QUERY</th>
                            <th>ROWS</th>
                            <th>CSV MEDIAN</th>
                            <th>PARQUET MEDIAN</th>
                            <th>SPEEDUP</th>
                          </tr>
                        </thead>
                        <tbody>
                          {Object.entries(benchmarkResult.query_benchmarks?.queries || {}).map(([key, q]) => (
                            <tr key={key}>
                              <td><strong>{q.title}</strong></td>
                              <td style={{ fontFamily: 'var(--font-mono)' }}>{formatNumber(q.row_count)}</td>
                              <td style={{ fontFamily: 'var(--font-mono)' }}>{q.csv_timings_ms.median_ms} ms</td>
                              <td style={{ fontFamily: 'var(--font-mono)', color: 'var(--color-primary)' }}>
                                {q.parquet_timings_ms.median_ms} ms
                              </td>
                              <td>
                                <span className="status-tag success">{q.speedup_ratio}x faster</span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </>
            )}

            {/* TAB 2: Demonstration PySpark Pipeline & DuckDB Queries */}
            {evalTab === 'spark' && (
              <div style={{ marginTop: '16px' }}>
                <div style={{ padding: '16px 20px', backgroundColor: 'var(--color-accent-light)', border: '1px solid var(--color-accent-border)', borderRadius: '10px', marginBottom: '24px' }}>
                  <strong>Demonstration & Regression Test Fixture:</strong> This PySpark pipeline validates local distributed computing using S3A connectors to MinIO. Generic Nimbus processing operates on multi-format files in the Processing tab.
                </div>

                <div style={{ display: 'flex', gap: '12px', alignItems: 'center', marginBottom: '24px' }}>
                  <button
                    type="button"
                    className="btn btn-primary"
                    onClick={runSparkPipeline}
                    disabled={sparkLoading}
                  >
                    <span className={sparkLoading ? 'spin' : ''}>
                      {sparkLoading ? '↻' : '▶'}
                    </span>
                    <span>{sparkLoading ? 'Running Spark Pipeline…' : 'Run PySpark Sales Pipeline'}</span>
                  </button>
                </div>

                {sparkError && (
                  <div className="alert-banner error">
                    <span>⚠ {sparkError}</span>
                  </div>
                )}

                {sparkResult && (
                  <div className="stats-summary-grid" style={{ marginBottom: '24px' }}>
                    <div className="stat-box">
                      <span className="stat-box-label">INPUT ROWS</span>
                      <span className="stat-box-value">{formatNumber(sparkResult.input_rows)}</span>
                      <span className="stat-box-detail">Read from raw-zone/sales.csv</span>
                    </div>
                    <div className="stat-box">
                      <span className="stat-box-label">CLEANED ROWS</span>
                      <span className="stat-box-value">{formatNumber(sparkResult.cleaned_rows)}</span>
                      <span className="stat-box-detail">Filtered & deduplicated</span>
                    </div>
                    <div className="stat-box">
                      <span className="stat-box-label">EXECUTION DURATION</span>
                      <span className="stat-box-value">{sparkResult.duration_seconds}s</span>
                      <span className="stat-box-detail">Mode: {sparkResult.execution_mode}</span>
                    </div>
                  </div>
                )}

                {/* DuckDB SQL Query Selector */}
                <h3 style={{ fontSize: '17px', margin: '24px 0 12px 0' }}>DuckDB In-Process Analytics Query</h3>
                <div style={{ display: 'flex', gap: '12px', alignItems: 'center', flexWrap: 'wrap', marginBottom: '16px' }}>
                  <select
                    className="picker-select-control"
                    value={duckdbQuery}
                    onChange={(e) => setDuckdbQuery(e.target.value)}
                    disabled={duckdbLoading}
                  >
                    <option value="sales_by_city">Sales by city</option>
                    <option value="orders_by_city">Orders by city</option>
                    <option value="top_cities_by_sales">Top cities by sales</option>
                    <option value="high_value_orders">High-value orders (≥ 1000)</option>
                  </select>
                  <button
                    type="button"
                    className="btn btn-secondary"
                    onClick={() => runDuckDBQuery(duckdbQuery)}
                    disabled={duckdbLoading}
                  >
                    {duckdbLoading ? 'Executing…' : 'Run DuckDB Query'}
                  </button>
                </div>

                {duckdbError && <div className="alert-banner error">⚠ {duckdbError}</div>}

                {duckdbResult?.queries?.[duckdbQuery] && (
                  <div className="table-card-container">
                    <table className="custom-table">
                      <thead>
                        <tr>
                          {duckdbResult.queries[duckdbQuery].columns.map((c) => (
                            <th key={c}>{c.toUpperCase()}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {duckdbResult.queries[duckdbQuery].rows.map((row, i) => (
                          <tr key={i}>
                            {duckdbResult.queries[duckdbQuery].columns.map((c) => (
                              <td key={c} style={{ fontFamily: 'var(--font-mono)' }}>
                                {String(row[c] ?? '—')}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}
          </div>
        )}

        {/* -------------------------------------------------------------
            PAGE: Custom 404
            ------------------------------------------------------------- */}
        {activePage === '404' && (
          <div className="notfound-container">
            <img
              src={notFoundBroomImg}
              alt="Broomstick Flew Off Course"
              className="notfound-illustration floating-broom"
            />
            <div className="notfound-code">ERROR 404</div>
            <h1 className="notfound-title">Looks like this page flew off.</h1>
            <p className="notfound-message">
              The destination or object path you requested does not exist in the Nimbus data lake.
            </p>
            <div className="notfound-actions">
              <button
                type="button"
                className="btn btn-primary"
                onClick={() => navigate('#/overview')}
              >
                Back to Overview
              </button>
              <button
                type="button"
                className="btn btn-secondary"
                onClick={() => { handleSelectZone('raw-zone'); navigate('#/storage/raw-zone'); }}
              >
                Open Storage Browser
              </button>
            </div>
          </div>
        )}
      </main>

      {/* -------------------------------------------------------------
          RIGHT-SIDE PREVIEW DRAWER
          ------------------------------------------------------------- */}
      {drawerOpen && (
        <div
          className="preview-drawer-backdrop"
          onClick={closePreviewDrawer}
          role="dialog"
          aria-modal="true"
          aria-label={`Preview for ${drawerObject}`}
        >
          <div
            className={`preview-drawer-panel ${drawerExpanded ? 'expanded-width' : ''}`}
            onClick={(e) => e.stopPropagation()}
          >
            {/* Drawer Header */}
            <div className="drawer-header">
              <div className="drawer-title-group">
                <div className="drawer-meta-pills">
                  <span className="gateway-stats-pill">{drawerBucket}</span>
                  {drawerResult && <FileTypeBadge ext={drawerResult.format} />}
                </div>
                <h2 className="drawer-filename">{drawerObject}</h2>
              </div>

              <div className="drawer-controls-group">
                {drawerBucket === 'raw-zone' && (
                  <button
                    type="button"
                    className="btn btn-primary btn-sm"
                    onClick={() => {
                      closePreviewDrawer()
                      setProcessSelectedFile(drawerObject)
                      navigate(`#/processing?file=${encodeURIComponent(drawerObject)}&bucket=raw-zone`)
                    }}
                    title="Process this file in Processing view"
                  >
                    Process File →
                  </button>
                )}
                <button
                  type="button"
                  className="btn-icon-only"
                  onClick={() => setDrawerExpanded((prev) => !prev)}
                  title={drawerExpanded ? 'Normal width' : 'Expand full width'}
                  aria-label="Toggle drawer width"
                >
                  ⤢
                </button>
                <button
                  type="button"
                  className="btn-icon-only"
                  onClick={closePreviewDrawer}
                  aria-label="Close preview drawer"
                >
                  ✕
                </button>
              </div>
            </div>

            {/* Drawer Body */}
            <div className="drawer-scroll-body">
              {drawerLoading && (
                <div style={{ textAlign: 'center', padding: '60px', color: 'var(--text-muted)' }}>
                  <span className="spin" style={{ fontSize: '24px', display: 'block', marginBottom: '12px' }}>↻</span>
                  Parsing and extracting preview…
                </div>
              )}

              {drawerError && (
                <div className="alert-banner error">
                  <span>⚠ {drawerError}</span>
                </div>
              )}

              {drawerResult && (
                <>
                  {/* Metrics Strip */}
                  <div className="drawer-metrics-strip">
                    <div className="metric-strip-item">
                      <span className="metric-strip-label">FORMAT</span>
                      <span className="metric-strip-value">{drawerResult.format}</span>
                    </div>
                    <div className="metric-strip-item">
                      <span className="metric-strip-label">RECORDS / PAGES</span>
                      <span className="metric-strip-value">{formatNumber(drawerResult.record_or_page_count)}</span>
                    </div>
                    {drawerResult.preview_duration_ms != null && (
                      <div className="metric-strip-item">
                        <span className="metric-strip-label">PARSE TIME</span>
                        <span className="metric-strip-value">{drawerResult.preview_duration_ms} ms</span>
                      </div>
                    )}
                  </div>

                  {/* Partial Extraction Warning */}
                  {drawerResult.status === 'partial' && (
                    <div className="partial-alert">
                      <span style={{ fontSize: '18px' }}>⚠</span>
                      <div>
                        <strong>Partial Extraction Notice:</strong> Scanned media or textless PDFs have no digital text layer. Full text extraction requires Optical Character Recognition (OCR). Available metadata has been extracted.
                      </div>
                    </div>
                  )}

                  {/* Format Renderers */}
                  {/* CSV / Parquet Table */}
                  {(drawerResult.format === 'CSV' || drawerResult.format === 'PARQUET') &&
                    Array.isArray(drawerResult.extracted_preview) &&
                    drawerResult.extracted_preview.length > 0 && (
                      <div>
                        <h4 style={{ fontSize: '14px', marginBottom: '8px' }}>Extracted Table Sample:</h4>
                        <div className="drawer-table-wrap">
                          <table className="custom-table">
                            <thead>
                              <tr>
                                {Object.keys(drawerResult.extracted_preview[0]).map((h) => (
                                  <th key={h}>{h.toUpperCase()}</th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {drawerResult.extracted_preview.map((row, rIdx) => (
                                <tr key={rIdx}>
                                  {Object.keys(drawerResult.extracted_preview[0]).map((h) => (
                                    <td key={h} style={{ fontFamily: 'var(--font-mono)', fontSize: '13px' }}>
                                      {String(row[h] ?? '—')}
                                    </td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                  {/* PDF Pages */}
                  {drawerResult.format === 'PDF' && Array.isArray(drawerResult.extracted_preview) && (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '14px' }}>
                      <h4 style={{ fontSize: '14px', margin: 0 }}>Document Text Pages:</h4>
                      {drawerResult.extracted_preview.map((p, idx) => (
                        <div key={idx} style={{ background: 'var(--bg-surface-subtle)', border: '1px solid var(--border-subtle)', borderRadius: '10px', padding: '14px' }}>
                          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px', fontSize: '12px', fontWeight: 600, color: 'var(--text-muted)' }}>
                            <span>PAGE {p.page_number ?? idx + 1}</span>
                            <span>{p.text ? `${p.text.length} chars` : 'No text stream'}</span>
                          </div>
                          {p.text ? (
                            <pre className="preformatted-text-box" style={{ maxHeight: '200px' }}>{p.text}</pre>
                          ) : (
                            <span style={{ fontSize: '13px', color: 'var(--color-warning)' }}>
                              ⚠ Scanned page without digital text layer. OCR required.
                            </span>
                          )}
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Images */}
                  {['JPG', 'PNG'].includes(drawerResult.format) && (
                    <div style={{ padding: '20px', background: 'var(--bg-surface-subtle)', border: '1px solid var(--border-subtle)', borderRadius: '12px', textAlign: 'center' }}>
                      <div style={{ fontSize: '32px', marginBottom: '8px' }}>🖼</div>
                      <h4 style={{ margin: '0 0 4px 0' }}>Raster Image Verified</h4>
                      <p style={{ margin: 0, fontSize: '13px', color: 'var(--text-muted)' }}>
                        Dimensions and color space parsed successfully via Pillow.
                      </p>
                    </div>
                  )}

                  {/* Generic JSON / Text / Log preview */}
                  {!['CSV', 'PARQUET', 'PDF', 'JPG', 'PNG'].includes(drawerResult.format) && (
                    <div>
                      <h4 style={{ fontSize: '14px', marginBottom: '8px' }}>Extracted Payload:</h4>
                      <pre className="preformatted-text-box">
                        {JSON.stringify(drawerResult.extracted_preview, null, 2)}
                      </pre>
                    </div>
                  )}

                  {/* Metadata Panel */}
                  {drawerResult.metadata && Object.keys(drawerResult.metadata).length > 0 && (
                    <div>
                      <h4 style={{ fontSize: '14px', marginBottom: '8px' }}>Format Metadata & Properties:</h4>
                      <pre className="preformatted-text-box" style={{ maxHeight: '200px' }}>
                        {JSON.stringify(drawerResult.metadata, null, 2)}
                      </pre>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>
        </div>
      )}

      {/* -------------------------------------------------------------
          MODALS: Overwrite & Delete
          ------------------------------------------------------------- */}
      {pendingOverwrite && (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <div className="modal-dialog-box">
            <div className="modal-header-line">
              <div className="modal-icon-badge warning">⚠</div>
              <h3 className="modal-title">Confirm File Overwrite</h3>
            </div>
            <p className="modal-body-text">
              An object named <strong>{pendingOverwrite.file.name}</strong> already exists in <strong>{pendingOverwrite.bucket}</strong>. Uploading will replace the existing file.
            </p>
            <div className="modal-target-card">
              {pendingOverwrite.file.name} ({formatBytes(pendingOverwrite.file.size)})
            </div>
            <div className="modal-actions-row">
              <button
                type="button"
                className="btn btn-secondary"
                onClick={() => setPendingOverwrite(null)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="btn btn-primary"
                onClick={() => {
                  const target = pendingOverwrite
                  setPendingOverwrite(null)
                  uploadSingleFile({ id: Date.now(), file: target.file, name: target.file.name, size: target.file.size }, true)
                }}
              >
                Confirm Overwrite
              </button>
            </div>
          </div>
        </div>
      )}

      {pendingDelete && (
        <div className="modal-overlay" role="dialog" aria-modal="true">
          <div className="modal-dialog-box">
            <div className="modal-header-line">
              <div className="modal-icon-badge danger">🗑</div>
              <h3 className="modal-title">Confirm Object Deletion</h3>
            </div>
            <p className="modal-body-text">
              Are you sure you want to permanently delete this object from <strong>{pendingDelete.bucket}</strong>? This action cannot be undone.
            </p>
            <div className="modal-target-card">
              {pendingDelete.objectName}
            </div>
            <div className="modal-actions-row">
              <button
                type="button"
                className="btn btn-secondary"
                onClick={() => setPendingDelete(null)}
                disabled={deleteLoading}
              >
                Cancel
              </button>
              <button
                type="button"
                className="btn btn-danger"
                onClick={executeDelete}
                disabled={deleteLoading}
              >
                {deleteLoading ? 'Deleting…' : 'Delete Object'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* -------------------------------------------------------------
          FOOTER
          ------------------------------------------------------------- */}
      <footer className="app-footer">
        <span className="footer-brand">
          Nimbus <span style={{ color: 'var(--color-accent)' }}>✦</span> Local S3-Compatible Cloud Data Lake
        </span>
        <span>MinIO Object Storage • FastAPI • React/Vite</span>
      </footer>
    </div>
  )
}

// -------------------------------------------------------------
// Row Actions Three-Dot Popover Menu
// -------------------------------------------------------------
function RowActionsMenu({
  objectName,
  bucket,
  onPreview,
  onProcess,
  onDownload,
  onDelete,
}) {
  const [open, setOpen] = useState(false)
  const [position, setPosition] = useState({
    top: null,
    bottom: null,
    left: null,
    right: null,
    placement: 'down',
  })
  const triggerRef = useRef(null)
  const menuRef = useRef(null)

  const updatePosition = useCallback(() => {
    if (!triggerRef.current) return
    const rect = triggerRef.current.getBoundingClientRect()

    // Dismiss if trigger button scrolled completely outside viewport
    if (rect.bottom < 0 || rect.top > window.innerHeight) {
      setOpen(false)
      return
    }

    const spaceBelow = window.innerHeight - rect.bottom
    const spaceAbove = rect.top
    const menuHeight = menuRef.current?.offsetHeight || 170
    const minSpace = Math.max(190, menuHeight + 12)
    const placeUp = spaceBelow < minSpace && spaceAbove > spaceBelow

    const rightDist = window.innerWidth - rect.right
    const fitsRight = (rect.right - 170) >= 8

    setPosition({
      placement: placeUp ? 'up' : 'down',
      top: placeUp ? null : Math.round(rect.bottom + 4),
      bottom: placeUp ? Math.round(window.innerHeight - rect.top + 4) : null,
      left: !fitsRight ? Math.max(8, Math.round(rect.left)) : null,
      right: fitsRight ? Math.max(8, Math.round(rightDist)) : null,
    })
  }, [])

  const handleToggle = (e) => {
    e.stopPropagation()
    if (!open) {
      updatePosition()
      setOpen(true)
    } else {
      setOpen(false)
    }
  }

  useEffect(() => {
    if (!open) return

    updatePosition()

    const handleScroll = () => {
      updatePosition()
    }

    const handleResize = () => {
      updatePosition()
    }

    const handleClickOutside = (e) => {
      if (triggerRef.current?.contains(e.target)) return
      if (menuRef.current?.contains(e.target)) return
      setOpen(false)
    }

    const handleKeyDown = (e) => {
      if (e.key === 'Escape') {
        setOpen(false)
      }
    }

    window.addEventListener('scroll', handleScroll, true)
    window.addEventListener('resize', handleResize)
    document.addEventListener('mousedown', handleClickOutside)
    document.addEventListener('touchstart', handleClickOutside)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      window.removeEventListener('scroll', handleScroll, true)
      window.removeEventListener('resize', handleResize)
      document.removeEventListener('mousedown', handleClickOutside)
      document.removeEventListener('touchstart', handleClickOutside)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [open, updatePosition])

  return (
    <div className="actions-menu-wrap">
      <button
        ref={triggerRef}
        type="button"
        className={`three-dot-btn ${open ? 'active' : ''}`}
        onClick={handleToggle}
        aria-label={`Actions for ${objectName}`}
        aria-expanded={open}
        aria-haspopup="menu"
      >
        <DotsIcon />
      </button>

      {open &&
        createPortal(
          <div
            ref={menuRef}
            className={`actions-dropdown-popover ${position.placement === 'up' ? 'placement-up' : 'placement-down'}`}
            style={{
              top: position.top !== null ? `${position.top}px` : undefined,
              bottom: position.bottom !== null ? `${position.bottom}px` : undefined,
              left: position.left !== null ? `${position.left}px` : undefined,
              right: position.right !== null ? `${position.right}px` : undefined,
            }}
            role="menu"
            aria-label={`Actions for ${objectName}`}
          >
            {onPreview && (
              <button
                type="button"
                className="action-menu-item"
                role="menuitem"
                onClick={() => {
                  setOpen(false)
                  onPreview()
                }}
              >
                👁 Preview
              </button>
            )}

            {bucket === 'raw-zone' && onProcess && (
              <button
                type="button"
                className="action-menu-item"
                role="menuitem"
                onClick={() => {
                  setOpen(false)
                  onProcess()
                }}
              >
                ▶ Process File
              </button>
            )}

            {onDownload && (
              <button
                type="button"
                className="action-menu-item"
                role="menuitem"
                onClick={() => {
                  setOpen(false)
                  onDownload()
                }}
              >
                ↓ Download
              </button>
            )}

            {onDelete && (
              <button
                type="button"
                className="action-menu-item danger"
                role="menuitem"
                onClick={() => {
                  setOpen(false)
                  onDelete()
                }}
              >
                × Delete
              </button>
            )}
          </div>,
          document.body
        )}
    </div>
  )
}

export default App
