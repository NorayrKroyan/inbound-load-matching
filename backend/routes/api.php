<?php

use Illuminate\Support\Facades\Route;
use App\Http\Controllers\Api\InboundLoadMatchingController;

/*
|--------------------------------------------------------------------------
| Health
|--------------------------------------------------------------------------
*/
Route::get('/health', fn () => response()->json(['ok' => true]));

//Inbound loads
Route::get('/inbound-loads/queue', [InboundLoadMatchingController::class, 'queue']);
Route::post('/inbound-loads/process', [InboundLoadMatchingController::class, 'process']);
Route::post('/inbound-loads/process-batch', [InboundLoadMatchingController::class, 'processBatch']);
Route::get('/inbound-loads/logs', [InboundLoadMatchingController::class, 'logs']);

Route::get('/inbound-loads/autoprocess/status', [InboundLoadMatchingController::class, 'autoProcessStatus']);
Route::post('/inbound-loads/autoprocess/start', [InboundLoadMatchingController::class, 'autoProcessStart']);
Route::post('/inbound-loads/autoprocess/stop', [InboundLoadMatchingController::class, 'autoProcessStop']);

